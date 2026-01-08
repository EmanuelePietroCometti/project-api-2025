use crate::file_api::{DirectoryEntry, FileApi};
use anyhow::Result;
use fuser015::{
    spawn_mount2, FileAttr, FileType, Filesystem, MountOption, Notifier, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use libc::{EIO, ENOENT, ENOTDIR, ENOTEMPTY};
use rust_socketio::{ClientBuilder, Payload};
use serde_json::Value;
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{mpsc::channel, Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Runtime;

const TTL: Duration = Duration::from_millis(2000);

#[derive(Debug, Clone, Copy)]
struct HttpStatus(pub u16);
impl std::fmt::Display for HttpStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "http status {}", self.0)
    }
}

impl std::error::Error for HttpStatus {}

pub(crate) struct TempWrite {
    tem_path: PathBuf,
    size: u64,
    dirty: bool,
}

#[derive(Clone)]
pub(crate) struct FsState {
    pub ino_by_path: Arc<Mutex<HashMap<PathBuf, u64>>>,
    pub path_by_ino: Arc<Mutex<HashMap<u64, PathBuf>>>,
    pub attr_cache: Arc<Mutex<HashMap<PathBuf, FileAttr>>>,
    pub dir_cache: Arc<Mutex<HashMap<PathBuf, (Vec<DirectoryEntry>, SystemTime)>>>,
    pub writes: Arc<Mutex<HashMap<u64, TempWrite>>>,
    pub next_ino: Arc<Mutex<u64>>,
    pub cache_ttl: Duration,
    pub next_fh: Arc<AtomicU64>,
}

struct RemoteFs {
    state: Arc<FsState>,
    api: FileApi,
    rt: Arc<Runtime>,
    notifier: Arc<Mutex<Option<Notifier>>>,
}

fn errno_from_anyhow(err: &anyhow::Error) -> i32 {
    use libc::{EACCES, EEXIST, EINVAL, EIO, ENOENT, ENOSPC};
    for cause in err.chain() {
        if let Some(ioe) = cause.downcast_ref::<std::io::Error>() {
            return match ioe.kind() {
                std::io::ErrorKind::PermissionDenied => EACCES,
                std::io::ErrorKind::NotFound => ENOENT,
                std::io::ErrorKind::AlreadyExists => EEXIST,
                std::io::ErrorKind::InvalidInput => EINVAL,
                std::io::ErrorKind::WriteZero
                | std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset => EIO,
                _ => EIO,
            };
        }
        if let Some(HttpStatus(code)) = cause.downcast_ref::<HttpStatus>() {
            return match *code {
                409 => EEXIST,       // Already exist
                404 => ENOENT,       // Not found
                401 | 403 => EACCES, // Permissions/Authorization
                400 => EINVAL,       // Invalid arguments
                507 | 413 => ENOSPC, // Too large
                _ => EIO,            // Server error
            };
        }
    }
    EIO
}

// Function that extract the file's metadata from the payload
fn metadata_from_payload(payload: &Value) -> Option<(PathBuf, String, bool, u64, i64, u16, i64)> {
    let rel = payload["relPath"].as_str()?;
    let name = payload["name"]
        .as_str()
        .map(|s| s.to_string())
        .or_else(|| {
            Path::new(rel)
                .file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        })?;

    let is_dir = payload["is_dir"].as_bool().unwrap_or(false);
    let size = payload["size"].as_u64().unwrap_or(0);
    let mtime = payload["mtime"].as_i64().unwrap_or(0);

    let perm_str = payload["permissions"].as_str().unwrap_or("644");
    let perm = u16::from_str_radix(perm_str, 8).unwrap_or(0o644);
    let nlink = payload["nlink"].as_i64().unwrap_or(1);
    let abs = Path::new("/").join(rel);

    Some((abs, name, is_dir, size, mtime, perm, nlink))
}

// Function that start the websocket listener, initialize the websocket connection and listen the messages
pub fn start_websocket_listener(api_url: &str, notifier: Arc<Notifier>, fs_state: Arc<FsState>) {
    let ws_url = format!("{}/socket.io/", api_url.trim_end_matches('/'));

    tokio::spawn(async move {
        let notifier_cloned = notifier.clone();
        let fs_state_cloned = fs_state.clone();
        let ws_url = ws_url.clone();
        tokio::task::spawn_blocking(move || {
            let client = ClientBuilder::new(ws_url)
                .on("connect", |_, _| {})
                .on("fs_change", move |payload, _| match payload {
                    Payload::Text(values) => {
                        if values.len() < 1 {
                            return;
                        }
                        let json_payload = &values[0];
                        handle_fs_change(json_payload, &notifier_cloned, &fs_state_cloned);
                    }
                    _other => {
                    }
                })
                .on("error", |_err, _| {})
                .connect();

            if let Err(_err) = client {}
        });
    });
}

/// Returns the parent inode and file name for a given path, or None if the path has no parent
fn resolve_parent<'a>(path: &'a Path, st: &FsState) -> Option<(u64, &'a std::ffi::OsStr)> {
    let parent = path.parent()?;
    let name = path.file_name()?;
    let ino = st.ino_of(parent)?;
    Some((ino, name))
}

/// Dispatches a filesystem-change WebSocket event to the correct handler based on its operation type.
fn handle_fs_change(payload: &serde_json::Value, notifier: &Notifier, fs_state: &FsState) {
    let op = payload["op"].as_str().unwrap_or("");
    match op {
        "add" | "addDir" => {
            handle_created(payload, notifier, fs_state);
        }

        "write" | "change" => {
            handle_updated(payload, notifier, fs_state);
        }

        "unlink" | "unlinkDir" => {
            handle_deleted_event(payload, notifier, fs_state);
        }

        "rename" | "renameDir" => {
            handle_renamed_event(payload, notifier, fs_state);
        }

        _ => {
            return;
        }
    }
}

/// Handles a "create" event by inserting the new file/dir metadata and invalidating relevant caches
fn handle_created(payload: &Value, notifier: &Notifier, st: &FsState) {
    let Some((abs, name, is_dir, size, mtime, perm, nlink)) = metadata_from_payload(payload) else {
        return;
    };
    if let Some(_existing_ino) = st.ino_of(&abs) {
        update_cache_from_metadata(st, &abs, &name, is_dir, size, mtime, perm, nlink);
        return; 
    }
    let ino = update_cache_from_metadata(st, &abs, &name, is_dir, size, mtime, perm, nlink);
    let parent = abs.parent().unwrap_or(Path::new("/"));
    if let Some(parent_ino) = st.ino_of(parent) {
        st.remove_dir_cache(parent);
        let _ = notifier.inval_entry(parent_ino, OsStr::new(&name));
    }
    let _ = notifier.inval_inode(ino, 0, 0);
}
/// Handles a delete event by resolving the absolute path and delegating removal logic
fn handle_deleted_event(payload: &Value, notifier: &Notifier, st: &FsState) {
    if let Some(rel) = payload["relPath"].as_str() {
        let abs = Path::new("/").join(rel);
        handle_deleted_path(&abs, notifier, st);
    }
}

/// Removes all cached state for a deleted path and notifies FUSE of invalidated entries
fn handle_deleted_path(abs: &Path, notifier: &Notifier, st: &FsState) {
    if let Some((parent_ino, name)) = resolve_parent(abs, st) {
        let _ = notifier.inval_entry(parent_ino, name);
        let _ = notifier.inval_inode(parent_ino, 0, 0);
    }

    st.remove_path(abs);
    st.remove_attr(abs);

    if let Some(parent) = abs.parent() {
        st.remove_dir_cache(parent);
    }
}

/// Handles a rename event by updating inode-path mappings and invalidating affected caches
fn handle_renamed_event(payload: &Value, notifier: &Notifier, st: &FsState) {
    let Some(old_rel) = payload["oldPath"].as_str() else {
        return;
    };
    let Some(new_rel) = payload["newPath"].as_str() else {
        return;
    };

    let old_abs = Path::new("/").join(old_rel);
    let new_abs = Path::new("/").join(new_rel);

    if let Some((old_parent_ino, old_name)) = resolve_parent(&old_abs, st) {
        let _ = notifier.inval_entry(old_parent_ino, old_name);
        let _ = notifier.inval_inode(old_parent_ino, 0, 0);
    }

    let ino = if let Some(ino) = st.ino_of(&old_abs) {
        st.remove_path(&old_abs);
        st.insert_path_mapping(&new_abs, ino);
        ino
    } else {
        st.ino_of(&new_abs)
            .unwrap_or_else(|| st.allocate_ino(&new_abs))
    };

    let Some((_abs_meta, name, is_dir, size, mtime, perm, nlink)) = metadata_from_payload(payload)
    else {
        st.remove_attr(&old_abs);
        st.remove_attr(&new_abs);
        return;
    };
    let final_abs = &new_abs;
    let _ = update_cache_from_metadata(st, final_abs, &name, is_dir, size, mtime, perm, nlink);
    if let Some((new_parent_ino, _)) = resolve_parent(&new_abs, st) {
        let _ = notifier.inval_inode(new_parent_ino, 0, 0);
    }

    let _ = notifier.inval_inode(ino, 0, 0);
}

/// Handles a file update event by refreshing attributes and invalidating the inode in FUSE
fn handle_updated(payload: &Value, notifier: &Notifier, st: &FsState) {
    let Some((abs, name, is_dir, size, mtime, perm, nlink)) = metadata_from_payload(payload) else {
        return;
    };
    let ino = update_cache_from_metadata(st, &abs, &name, is_dir, size, mtime, perm, nlink);
    let _ = notifier.inval_inode(ino, 0, 0);
}

/// Updates metadata caches based on remote API info and returns the inode associated with the path
pub fn update_cache_from_metadata(
    st: &FsState,
    abs: &Path,
    name: &str,
    is_dir: bool,
    size: u64,
    mtime: i64,
    perm: u16,
    nlink: i64,
) -> u64 {
    let kind = if is_dir {
        FileType::Directory
    } else {
        FileType::RegularFile
    };
    let parent = abs.parent().unwrap_or(Path::new("/"));
    let ino = match st.ino_of(abs) {
        Some(i) => i,
        None => st.allocate_ino(abs),
    };

    let blocks = if size == 0 { 0 } else { (size + 511) / 512 };
    let uid = (unsafe { libc::getuid() }) as u32;
    let gid = (unsafe { libc::getgid() }) as u32;
    if st.get_attr(parent).is_none() {
        let attr = FileAttr {
            ino,
            size,
            blocks,
            blksize: 512,
            atime: UNIX_EPOCH + Duration::from_secs(mtime as u64),
            mtime: UNIX_EPOCH + Duration::from_secs(mtime as u64),
            ctime: UNIX_EPOCH + Duration::from_secs(mtime as u64),
            crtime: UNIX_EPOCH + Duration::from_secs(mtime as u64),
            kind,
            perm,
            nlink: nlink as u32,
            uid,
            gid,
            rdev: 0,
            flags: 0,
        };
        st.set_attr(abs, attr);
        st.insert_child(parent, name.to_string(), ino);
        st.remove_dir_cache(parent);
        ino
    } else {
        st.remove_attr(parent);
        st.remove_dir_cache(parent);
        ino
    }
}

impl FsState {
    fn new(_api: FileApi, _rt: Arc<Runtime>) -> Self {
        let mut ino_by_path = HashMap::new();
        let mut path_by_ino = HashMap::new();
        ino_by_path.insert(PathBuf::from("/"), 1);
        path_by_ino.insert(1, PathBuf::from("/"));
        Self {
            ino_by_path: Arc::new(Mutex::new(ino_by_path)),
            path_by_ino: Arc::new(Mutex::new(path_by_ino)),
            attr_cache: Arc::new(Mutex::new(HashMap::new())),
            dir_cache: Arc::new(Mutex::new(HashMap::new())),
            writes: Arc::new(Mutex::new(HashMap::new())),
            next_ino: Arc::new(Mutex::new(2)),
            cache_ttl: TTL,
            next_fh: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn insert_child(&self, parent: &Path, name: String, ino: u64) {
        let mut ino_by_path = self.ino_by_path.lock().unwrap();
        let mut path_by_ino = self.path_by_ino.lock().unwrap();

        let mut child = parent.to_path_buf();
        if child.to_string_lossy() != "/" {
            child.push(name);
        } else {
            child = PathBuf::from(format!("/{}", name));
        }

        ino_by_path.insert(child.clone(), ino);
        path_by_ino.insert(ino, child);
    }

    pub fn insert_write_tempfile(&self, fh: u64, temp_path: PathBuf, dirty: bool) {
        let mut writes = self.writes.lock().unwrap();
        writes.insert(
            fh,
            TempWrite {
                tem_path: temp_path,
                size: 0,
                dirty,
            },
        );
    }

    // Accesso MUTABILE (per write, flush, release)
    fn with_write_mut<F, R>(&self, fh: u64, f: F) -> Option<R>
    where
        F: FnOnce(&mut TempWrite) -> R,
    {
        let mut writes = self.writes.lock().unwrap();
        writes.get_mut(&fh).map(f)
    }

    // Rimuove e restituisce (solo in release)
    pub fn take_write(&self, fh: u64) -> Option<TempWrite> {
        self.writes.lock().unwrap().remove(&fh)
    }

    // ---- PATH ↔ INODE ----

    pub fn ino_of(&self, path: &Path) -> Option<u64> {
        self.ino_by_path.lock().unwrap().get(path).cloned()
    }

    pub fn path_of(&self, ino: u64) -> Option<PathBuf> {
        self.path_by_ino.lock().unwrap().get(&ino).cloned()
    }

    pub fn allocate_ino(&self, path: &Path) -> u64 {
        let mut next = self.next_ino.lock().unwrap();
        let ino = *next;
        *next += 1;
        self.ino_by_path
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), ino);
        self.path_by_ino
            .lock()
            .unwrap()
            .insert(ino, path.to_path_buf());
        ino
    }

    pub fn remove_path(&self, path: &Path) {
        if let Some(ino) = self.ino_by_path.lock().unwrap().remove(path) {
            self.path_by_ino.lock().unwrap().remove(&ino);
        }
    }

    pub fn insert_path_mapping(&self, path: &Path, ino: u64) {
        self.ino_by_path
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), ino);

        self.path_by_ino
            .lock()
            .unwrap()
            .insert(ino, path.to_path_buf());
    }

    // ---- CACHE ATTR ----

    pub fn get_attr(&self, path: &Path) -> Option<FileAttr> {
        self.attr_cache.lock().unwrap().get(path).cloned()
    }

    pub fn set_attr(&self, path: &Path, attr: FileAttr) {
        self.attr_cache
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), attr);
    }

    pub fn remove_attr(&self, path: &Path) {
        self.attr_cache.lock().unwrap().remove(path);
    }

    // ---- CACHE DIRECTORY ----

    pub fn get_dir_cache(&self, path: &Path) -> Option<(Vec<DirectoryEntry>, SystemTime)> {
        self.dir_cache.lock().unwrap().get(path).cloned()
    }

    pub fn set_dir_cache(&self, path: &Path, data: (Vec<DirectoryEntry>, SystemTime)) {
        self.dir_cache
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), data);
    }

    pub fn remove_dir_cache(&self, path: &Path) {
        self.dir_cache.lock().unwrap().remove(path);
    }

    // ---- CLEAR CACHE ----

    pub fn clear_all_cache(&self) {
        self.attr_cache.lock().unwrap().clear();
        self.dir_cache.lock().unwrap().clear();
    }

    pub fn cleanup_all_tempfiles(&self) {
        let writes = match self.writes.lock() {
            Ok(w) => w,
            Err(_) => {
                return;
            }
        };

        for (_ino, tw) in writes.iter() {
            if tw.tem_path.exists() {
                match std::fs::remove_file(&tw.tem_path) {
                    Ok(_) => {
                        continue;
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }

    pub fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }
}

impl RemoteFs {
    fn get_temporary_path(&self, ino: u64) -> PathBuf {
        let mut tmp_path = std::env::temp_dir();
        tmp_path.push(format!("tempfile_{}", ino));
        tmp_path
    }

    // Function that init the cache
    // It is called at the beginning
    pub fn init_cache(&self) {
        self.state.clear_all_cache();
    }

    // Function that verify if the cache is still valid
    pub fn is_cache_valid(&self, timestamp: SystemTime) -> bool {
        SystemTime::now().duration_since(timestamp).unwrap() < self.state.cache_ttl
    }

    pub fn get_attr_cache(&self, path: &Path) -> Option<FileAttr> {
        self.state.get_attr(&path)
    }

    // Function that allow to free the cache
    // If I pass a specific path, only the specific path is free
    pub fn clear_cache(&self, path: Option<&Path>) {
        match path {
            Some(p) => {
                self.state.remove_attr(&p);
                self.state.remove_dir_cache(&p);
            }
            None => {
                self.state.clear_all_cache();
            }
        }
    }

    // Function that update the cache
    pub fn update_cache(&self, dir: &Path) -> anyhow::Result<()> {
        let rel_db = Self::rel_for_db(dir);
        let rel_fs = Self::rel_for_fs(dir);
        let list = self.rt.block_on(self.api.ls(&rel_db))?;

        self.state
            .set_dir_cache(&dir.to_path_buf(), (list.clone(), SystemTime::now()));
        let rel_db_parent = Self::rel_for_db(dir);
        let de = self.rt.block_on(self.api.get_update_metadata(&rel_db_parent))?;
        if let Some(mut parent_attr) = self.get_attr_cache(dir) {
            parent_attr.nlink = de.nlink as u32;
            parent_attr.size = de.size as u64;
            parent_attr.mtime = UNIX_EPOCH + Duration::from_secs(de.mtime as u64);
            self.state.set_attr(dir, parent_attr);
        }
        for de in &list {
            let mut child = PathBuf::from("/");
            if !rel_fs.is_empty() {
                child.push(&rel_fs);
            }
            child.push(&de.name);

            let is_dir = Self::is_dir(&de);
            let ty = if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let nlink_child = de.nlink as u32;

            let perm = Self::parse_perm(&de.permissions);
            let size = de.size as u64;

            let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm, nlink_child);
            self.state.set_attr(&child, attr);
        }
        Ok(())
    }

    // Function that insert the state in the cache
    pub fn insert_attr_cache(&self, path: PathBuf, attr: FileAttr) {
        self.state.set_attr(&path, attr);
    }

    // Function that insert the folder state
    pub fn insert_dir_cache(&self, path: PathBuf, data: (Vec<DirectoryEntry>, SystemTime)) {
        self.state.set_dir_cache(&path, data);
    }

    // Function that create a new instance of RemoteFs
    fn new(api: FileApi, rt: Arc<Runtime>) -> Self {
        Self {
            state: Arc::new(FsState::new(api.clone(), rt.clone())),
            api,
            rt,
            notifier: Arc::new(Mutex::new(None)),
        }
    }

    // Function that allocate the inode
    fn alloc_ino(&self, path: &Path) -> u64 {
        if let Some(ino) = self.state.ino_of(path) {
            ino
        } else {
            self.state.allocate_ino(path)
        }
    }

    // Function that obtain the path from the inode
    fn path_of(&self, ino: u64) -> Option<PathBuf> {
        self.state.path_of(ino)
    }

    /// Extract relative path for db
    fn rel_for_db(path: &Path) -> String {
        let s = path.to_string_lossy();

        if s == "/" {
            return "".to_string();
        } else {
            let trimmed = s.trim_start_matches("/");
            format!("./{}", trimmed)
        }
    }

    /// Extract relative path for fs (PathBuf)

    fn rel_for_fs(path: &Path) -> String {
        let s = path.to_string_lossy();
        if s == "/" {
            "".to_string()
        } else {
            s.trim_start_matches('/').to_string()
        }
    }

    // Function that extract the file permissions
    fn file_attr(
        &self,
        path: &Path,
        ty: FileType,
        size: u64,
        mtime: Option<i64>,
        perm: u16,
        nlink: u32,
    ) -> FileAttr {
        let now = SystemTime::now();
        let mtime_st = mtime
            .and_then(|sec| SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(sec as u64)))
            .unwrap_or(now);
        let uid = (unsafe { libc::getuid() }) as u32;
        let gid = (unsafe { libc::getgid() }) as u32;
        FileAttr {
            ino: self.alloc_ino(path),
            size,
            blocks: (size + 511) / 512,
            atime: mtime_st,
            mtime: mtime_st,
            ctime: mtime_st,
            crtime: mtime_st,
            kind: ty,
            perm,
            nlink,
            uid,
            gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    // Function that transform the permissions in octal format
    fn parse_perm(permissions: &str) -> u16 {
        u16::from_str_radix(&permissions, 8).unwrap_or(0)
    }

    // Function that analyze the permissions and verify if we are working with a directory
    fn is_dir(de: &DirectoryEntry) -> bool {
        if de.is_dir == 1 {
            return true;
        }
        false
    }

    // Function that define the directory entries
    pub fn dir_entries(&self, dir: &Path) -> Result<Vec<(PathBuf, DirectoryEntry)>> {
        let rel_db = Self::rel_for_db(dir);
        let rel_fs = Self::rel_for_fs(dir);

        if let Some((entries, ts)) = self.state.get_dir_cache(&dir) {
            if self.is_cache_valid(ts) {
                let mut out = Vec::with_capacity(entries.len());
                for de in entries {
                    let mut child = PathBuf::from("/");
                    if !rel_fs.is_empty() {
                        child.push(&rel_fs);
                    }
                    child.push(&de.name);
                    out.push((child, de));
                }
                let _ = self.update_cache(dir);
                return Ok(out);
            }
        }

        let list = self.rt.block_on(self.api.ls(&rel_db))?;

        self.insert_dir_cache(dir.to_path_buf(), (list.clone(), SystemTime::now()));

        let mut out = Vec::with_capacity(list.len());

        for de in &list {
            let mut child = PathBuf::from("/");
            if !rel_fs.is_empty() {
                child.push(&rel_fs);
            }
            child.push(&de.name);

            let is_dir = Self::is_dir(&de);
            let ty = if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let perm = Self::parse_perm(&de.permissions);
            let size = de.size as u64;

            let child_nlink = de.nlink as u32;
            let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm, child_nlink);
            self.insert_attr_cache(child.clone(), attr);

            out.push((child, de.clone()));
        }
        let rel_db_parent = Self::rel_for_db(dir);
        let de = self.rt.block_on(self.api.get_update_metadata(&rel_db_parent))?;

        if let Some(mut parent_attr) = self.state.get_attr(dir) {
            parent_attr.nlink = de.nlink as u32;
            parent_attr.size = de.size as u64;
            parent_attr.mtime = UNIX_EPOCH + Duration::from_secs(de.mtime as u64);
            self.state.set_attr(dir, parent_attr);
        } else {
            
            let attr = self.file_attr(
                dir,
                FileType::Directory,
                de.size as u64,
                Some(de.mtime),
                0o755,
                de.nlink as u32,
            );
            self.state.set_attr(dir, attr);
        }

        Ok(out)
    }
}

impl Drop for RemoteFs {
    fn drop(&mut self) {
        self.state.cleanup_all_tempfiles();
    }
}

impl Filesystem for RemoteFs {
    // Function that update the file's attributes
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if ino == 1 {
            let uid = (unsafe { libc::getuid() }) as u32;
            let gid = (unsafe { libc::getgid() }) as u32;

            let mut attr = self.file_attr(Path::new("/"), FileType::Directory, 0, None, 0o755, 2);
            attr.uid = uid;
            attr.gid = gid;
            reply.attr(&self.state.cache_ttl, &attr);
            return;
        }
        let Some(path) = self.path_of(ino) else {
            reply.error(ENOENT);
            return;
        };
        let rel_db = Self::rel_for_db(&path);

        let mut attr = if let Some(a) = self.state.get_attr(&path) {
            a
        } else {
            let parent = path.parent().unwrap_or(Path::new("/"));
            let _ = self.dir_entries(parent);
            match self.state.get_attr(&path) {
                Some(a) => a,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };
        if let Some(m) = mode {
            attr.perm = (m & 0o777) as u16;
        }

        if let Some(new_size) = size {
            let mut is_local_write = false;

            if let Some(fh_val) = fh {
                if let Some(effective_size) = self.state.with_write_mut(fh_val, |tw| {
                    tw.size = tw.size.max(new_size);
                    tw.size
                }) {
                    attr.size = effective_size;
                    attr.blocks = (effective_size + 511) / 512;
                    is_local_write = true;
                }
            }

            if !is_local_write {
                match self.rt.block_on(self.api.truncate(&rel_db, new_size)) {
                    Ok(_) => {
                        attr.size = new_size;
                        attr.blocks = (new_size + 511) / 512;
                    }
                    Err(e) => {
                        let errno = errno_from_anyhow(&e);

                        if errno == libc::ENOENT || errno == libc::EIO || errno == libc::ENOSPC {
                            attr.size = new_size;
                            attr.blocks = (new_size + 511) / 512;
                        } else {
                            reply.error(errno);
                            return;
                        }
                    }
                }
            }
        }
        self.insert_attr_cache(path.to_path_buf(), attr.clone());
        reply.attr(&self.state.cache_ttl, &attr);
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser015::ReplyStatfs) {
        match self.rt.block_on(self.api.statfs()) {
            Ok(stats) => {
                let bsize = stats.bsize; // Block size (from backend)
                let blocks = stats.blocks; // Number of blocks (from backend)
                let bfree = stats.bfree; // Number of free blocks (from backend)
                let bavail = stats.bavail; // Available blocks (from backend)
                let files = stats.files; // Number of file nodes (from backend)
                let ffree = stats.ffree; // Number of free nodes (from backend)
                let namelen: u32 = 255; // Max length for file name (hardcoded)
                let frsize: u32 = bsize as u32; // Fragment size

                reply.statfs(
                    blocks,
                    bfree,
                    bavail,
                    files,
                    ffree,
                    bsize as u32,
                    namelen,
                    frsize,
                );
                return;
            }
            Err(_e) => {
                let bsize: u32 = 4096;
                let blocks: u64 = 1_000_000;
                let bfree: u64 = 1_000_000;
                let bavail: u64 = 1_000_000;
                let files: u64 = 1_000_000;
                let ffree: u64 = 1_000_000;
                let namelen: u32 = 255;
                let frsize: u32 = bsize;
                reply.statfs(blocks, bfree, bavail, files, ffree, bsize, namelen, frsize);
                return;
            }
        }
    }

    // Function that allow the research of file or directory
    fn lookup(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEntry,
    ) {
        let Some(parent_path) = self.path_of(parent) else {
            reply.error(ENOENT);
            return;
        };

        let child_path = parent_path.join(name);

        match self.dir_entries(&parent_path) {
            Ok(_) => {
                if let Some(attr) = self.state.get_attr(&child_path) {
                    reply.entry(&self.state.cache_ttl, &attr, 0);
                    return;
                } else {
                    reply.error(ENOENT);
                    return;
                }
            }
            Err(e) => {
                reply.error(errno_from_anyhow(&e));
                return;
            }
        }
    }

    // Retrieves the list of directory entries for a given path
    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let Some(dir) = self.path_of(ino) else {
            reply.error(ENOTDIR);
            return;
        };
        let entries = match self.dir_entries(&dir) {
            Ok(v) => v,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };
        if offset == 0 {
            if !reply.add(ino, 1, FileType::Directory, ".") {
                reply.ok();
                return;
            }
            let parent_ino = if dir == Path::new("/") {
                1
            } else {
                dir.parent()
                    .and_then(|p| self.state.ino_of(&p))
                    .unwrap_or(1)
            };
            if !reply.add(parent_ino, 2, FileType::Directory, "..") {
                reply.ok();
                return;
            }
        }
        let mut idx = if offset <= 2 {
            0
        } else {
            (offset - 2) as usize
        };
        while idx < entries.len() {
            let (child, de) = &entries[idx];
            let is_dir = Self::is_dir(&de);
            let ty = if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let child_ino = self.alloc_ino(child);
            let this_off = 3 + (idx as i64);
            if !reply.add(child_ino, this_off, ty, child.file_name().unwrap()) {
                break;
            }
            idx += 1;
        }

        reply.ok();
    }

    // Retrieves metadata and file attributes for a given path
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, fh: Option<u64>, reply: ReplyAttr) {
        if ino == 1 {
            let uid = (unsafe { libc::getuid() }) as u32;
            let gid = (unsafe { libc::getgid() }) as u32;
            let mut attr = self.file_attr(Path::new("/"), FileType::Directory, 0, None, 0o755, 2);
            attr.uid = uid;
            attr.gid = gid;
            reply.attr(&self.state.cache_ttl, &attr);
            return;
        }
        let Some(path) = self.path_of(ino) else {
            reply.error(ENOENT);
            return;
        };
        let mut forced_size: Option<u64> = None;
        if let Some(fh_val) = fh {
            let writes = self.state.writes.lock().unwrap();
            if let Some(tw) = writes.get(&fh_val) {
                forced_size = Some(tw.size);
            }
        }

        let attr_opt = if let Some(a) = self.state.get_attr(&path) {
            Some(a)
        } else {
            let parent = path.parent().unwrap_or(Path::new("/"));
            if self.dir_entries(parent).is_ok() {
                self.state.get_attr(&path)
            } else {
                None
            }
        };
        if let Some(mut attr) = attr_opt {
            if let Some(real_size) = forced_size {
                attr.size = real_size;
                attr.blocks = (real_size + 511) / 512;
            }
            reply.attr(&self.state.cache_ttl, &attr);
        } else {
            reply.error(ENOENT);
        }
    }

    // Function that open a new temporary file
    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        println!("[OPEN] Open called for ino: {}, flags: {}", ino, flags);
        let fh = self.state.alloc_fh();

        let wants_write = (flags & (libc::O_WRONLY | libc::O_RDWR)) != 0;

        if wants_write {
            let temp_path = self.get_temporary_path(fh);

            if let Err(_) = File::create(&temp_path) {
                reply.error(libc::EIO);
                return;
            }
            if let Some(path) = self.path_of(ino) {
                let Some(attr) = self.state.get_attr(&path) else {
                    reply.error(ENOENT);
                    return;
                };
                let rel = Self::rel_for_db(&path);
                if let Ok(bytes) = self.rt.block_on(self.api.read_all(&rel, attr.size)) {
                    if let Ok(mut f) = File::options().write(true).open(&temp_path) {
                        let _ = f.write_all(&bytes);
                    }
                }
            }
            self.state.insert_write_tempfile(fh, temp_path, true);
        }
        reply.opened(fh, flags as u32);
    }

    // Reads data from a file starting at a specified offset
    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let Some(path) = self.path_of(ino) else {
            reply.error(ENOENT);
            return;
        };

        let rel_db = Self::rel_for_db(&path);
        let mut tmp_result: Option<Vec<u8>> = None;
        let mut tmp_error: Option<i32> = None;

        self.state.with_write_mut(fh, |tw| {
            if let Ok(mut f) = File::open(&tw.tem_path) {
                let mut buf = vec![0u8; size as usize];
                if f.seek(SeekFrom::Start(offset.max(0) as u64)).is_ok() {
                    let n = f.read(&mut buf).unwrap_or(0);
                    buf.truncate(n);
                    tmp_result = Some(buf);
                } else {
                    tmp_error = Some(EIO);
                }
            } else {
                tmp_error = Some(EIO);
            }
        });
        if let Some(e) = tmp_error {
            reply.error(e);
            return;
        }
        if let Some(buf) = tmp_result {
            reply.data(&buf);
            return;
        }
        let attr = if let Some(a) = self.state.get_attr(&path) {
            Some(a)
        } else {
            let parent = path.parent().unwrap_or(Path::new("/"));
            let _ = self.dir_entries(parent);
            self.state.get_attr(&path)
        };

        let Some(attr) = attr else {
            reply.error(ENOENT);
            return;
        };

        if (offset as u64) >= attr.size {
            reply.data(&[]);
            return;
        }

        let start = offset.max(0) as u64;
        let end = (start + (size as u64) - 1).min(attr.size - 1);

        match self.rt.block_on(self.api.read_range(&rel_db, start, end)) {
            Ok(bytes) => reply.data(&bytes),
            Err(err) => reply.error(errno_from_anyhow(&err)),
        }
    }

    // Writes data to a file at a specified offset
    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        println!("[WRITE] Write called for fh: {}, offset: {}, size: {}", fh, offset, data.len());
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }

        let mut wrote = false;

        self.state.with_write_mut(fh, |tw| {
            if let Ok(mut f) = OpenOptions::new().write(true).open(&tw.tem_path) {
                if f.seek(SeekFrom::Start(offset as u64)).is_ok() && f.write_all(data).is_ok() {
                    let end = (offset as u64) + (data.len() as u64);
                    tw.size = tw.size.max(end);
                    tw.dirty=true;
                    wrote = true;
                }
            }
        });

        if wrote {
            reply.written(data.len() as u32);
        } else {
            reply.error(libc::EIO);
        }
    }

    // Ensures that any buffered file data is written to storage
    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        println!("[FLUSH] Flush called");
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        println!("[FSYNC] Fsync called for ino: {}, fh: {}", ino, fh);
        let write_info = {
            let writes = self.state.writes.lock().unwrap();
            writes.get(&fh).map(|tw| tw.tem_path.clone())
        };

        if let Some(tmp_path) = write_info {
            if let Some(path) = self.path_of(ino) {
                let rel = Self::rel_for_db(&path);
                // Sincronizza i dati col backend
                if let Err(e) = self
                    .rt
                    .block_on(self.api.write_file(&rel, &tmp_path.to_string_lossy()))
                {
                    reply.error(errno_from_anyhow(&e));
                    return;
                }
                self.state.with_write_mut(fh, |tw| tw.dirty = false);
            }
        }
        reply.ok();
    }

    // Closes a file and releases associated resources
    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        println!("[RELEASE] Release called for ino: {}, fh: {}", ino, fh);
        let Some(tw) = self.state.take_write(fh) else {
            reply.ok();
            return;
        };

        let path = match self.path_of(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let rel = Self::rel_for_db(&path);

        if tw.dirty {
            match self
                .rt
                .block_on(self.api.write_file(&rel, &tw.tem_path.to_string_lossy()))
            {
                Ok(_) => {
                    let size = std::fs::metadata(&tw.tem_path)
                        .map(|m| m.len())
                        .unwrap_or(0);

                    if let Some(mut attr) = self.state.get_attr(&path) {
                        attr.size = size;
                        attr.mtime = SystemTime::now();
                        attr.ctime = attr.mtime;
                        self.state.set_attr(&path, attr);
                    }

                    if let Some(parent) = path.parent() {
                        self.state.remove_dir_cache(parent);
                    }
                    println!("File {:?} written successfully.", path);
                    let _ = std::fs::remove_file(&tw.tem_path);
                    self.state.with_write_mut(fh, |tw| tw.dirty = false);
                    println!("Temporary file {:?} deleted.", tw.tem_path);
                }
                Err(_e) => {
                    let _ = std::fs::remove_file(&tw.tem_path);
                    reply.error(libc::EIO);
                    return;
                }
            }
        }
        reply.ok();
    }

    // Creates a new file with the given name and attributes
    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        println!(
            "[CREATE] Create called for parent ino: {}, name: {:?}, mode: {:o}, umask: {:o}",
            parent,
            name,
            mode,
            umask
        );
        let parent_path = match self.path_of(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let path = parent_path.join(name);
        let fh = self.state.alloc_fh();

        let mut tmp = std::env::temp_dir();
        tmp.push(format!("remote_fs_fh_{:x}.part", fh));

        let _ = std::fs::remove_file(&tmp);
        if std::fs::File::create(&tmp).is_err() {
            reply.error(libc::EIO);
            return;
        }

        self.state.insert_write_tempfile(fh, tmp, true);

        let final_mode = mode & !umask;
        let attr = self.file_attr(
            &path,
            FileType::RegularFile,
            0,
            None,
            (final_mode & 0o777) as u16,
            1,
        );

        self.state.set_attr(&path, attr.clone());
        if let Some(parent_path) = self.state.path_of(parent) {
            let _ = self.update_cache(&parent_path);
        }

        reply.created(&self.state.cache_ttl, &attr, 0, fh, 0);
    }

    // Changes the name or path of a file or directory
    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        println!(
            "[RENAME] Rename called from parent ino: {}, name: {:?} to new parent ino: {}, new name: {:?}",
            parent,
            name,
            newparent,
            newname
        );
        let old_parent = match self.path_of(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let new_parent = match self.path_of(newparent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let old_path = old_parent.join(name);
        let new_path = new_parent.join(newname);

        let old_rel = Self::rel_for_db(&old_path);
        let new_rel = Self::rel_for_db(&new_path);

        let target_ino_opt = self.state.ino_of(&new_path);
        match self.rt.block_on(self.api.rename(&old_rel, &new_rel)) {
            Ok(_) => {
                if let Some(ino) = self.state.ino_of(&old_path) {
                    self.state.remove_path(&old_path);
                    if let Some(_target_ino) = target_ino_opt {
                        self.state.remove_attr(&new_path);
                    }
                    if let Some(attr) = self.state.get_attr(&old_path) {
                        self.state.remove_attr(&old_path);
                        self.state.set_attr(&new_path, attr);
                    }
                    self.state.insert_path_mapping(&new_path, ino);
                }
                self.state.remove_dir_cache(&old_parent);
                if old_parent != new_parent {
                    self.state.remove_dir_cache(&new_parent);
                }
                reply.ok();
                return;
            }
            Err(e) => reply.error(errno_from_anyhow(&e)),
        }
    }

    // Creates a new directory at the specified path
    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let parent_path = match self.path_of(parent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let path = if parent_path == Path::new("/") {
            PathBuf::from("/").join(name)
        } else {
            parent_path.join(name)
        };

        let rel = Self::rel_for_db(&path);

        match self.rt.block_on(self.api.mkdir(&rel)) {
            Ok(_) => {
                if let Err(_e) = self.update_cache(&parent_path) {
                    reply.error(EIO);
                    return;
                }
                if let Some(attr) = self.state.get_attr(&path) {
                    reply.entry(&self.state.cache_ttl, &attr, 0);
                } else {
                    let attr = self.file_attr(&path, FileType::Directory, 64, None, 0o755, 2);
                    self.state.set_attr(&path, attr.clone());
                    reply.entry(&self.state.cache_ttl, &attr, 0);
                    return;
                }
            }
            Err(e) => {
                let errno = errno_from_anyhow(&e);
                reply.error(errno);
                return;
            }
        }
    }

    // Deletes a file from the filesystem
    fn unlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEmpty,
    ) {
        let Some(parent_path) = self.path_of(parent) else {
            reply.error(ENOENT);
            return;
        };
        let path = if parent_path == Path::new("/") {
            PathBuf::from("/").join(name)
        } else {
            parent_path.join(name)
        };

        let rel = Self::rel_for_db(&path);
        match self.rt.block_on(self.api.delete(&rel)) {
            Ok(_) => {
                self.clear_cache(Some(&path));
                let _ = self.update_cache(&parent_path);
                self.state.remove_path(&path); 
                reply.ok();
                return;
            }
            Err(e) => {
                let errno = errno_from_anyhow(&e);
                reply.error(errno);
                return;
            }
        }
    }

    // Removes an empty directory from the filesystem
    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = match self.path_of(parent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let path = if parent_path == Path::new("/") {
            PathBuf::from("/").join(name)
        } else {
            parent_path.join(name)
        };
        let is_dir = if let Some(attr) = self.state.get_attr(&path) {
            matches!(attr.kind, FileType::Directory)
        } else {
            match self.dir_entries(&path) {
                Ok(_) => true,
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };
        if !is_dir {
            reply.error(ENOTDIR);
            return;
        }
        match self.dir_entries(&path) {
            Ok(entries) if entries.is_empty() => {}
            Ok(_) => {
                reply.error(ENOTEMPTY);
                return;
            }
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        }

        let rel = Self::rel_for_db(&path);
        match self.rt.block_on(self.api.delete(&rel)) {
            Ok(_) => {
                self.clear_cache(Some(&path));
                self.state.remove_path(&path);
                let _ = self.update_cache(&parent_path);
                reply.ok();
                return;
            }
            Err(e) => {
                let errno = errno_from_anyhow(&e);
                reply.error(errno);
                return;
            }
        }
    }
}

pub fn mount_fs(mountpoint: &str, api: FileApi, url: String) -> anyhow::Result<()> {
    let rt = Arc::new(Runtime::new()?);

    let remote_fs = RemoteFs::new(api, rt.clone());

    let notifier_ptr = remote_fs.notifier.clone();
    let fs_state = remote_fs.state.clone();

    remote_fs.init_cache();

    let mp = mountpoint.to_string();
    let options = vec![
        MountOption::FSName("remote_fs".to_string()),
        MountOption::DefaultPermissions,
        MountOption::RW,
        MountOption::AutoUnmount,
        MountOption::Async,
    ];

    let bg_session = spawn_mount2(remote_fs, &mp, &options).expect("Failed to mount filesystem");

    let notifier_actual = bg_session.notifier();
    {
        let mut lock = notifier_ptr.lock().unwrap();
        *lock = Some(notifier_actual.clone());
    }
    {
        let url_clone = url.clone();
        let notifier_for_ws = Arc::new(notifier_actual);
        rt.spawn(async move {
            start_websocket_listener(&url_clone, notifier_for_ws, fs_state);
        });
    }

    let mut signals = Signals::new(&[SIGINT, SIGTERM])?;
    let shutting_down = Arc::new(AtomicBool::new(false));
    let (tx, rx) = channel();

    {
        let tx = tx.clone();
        let shutting_down = shutting_down.clone();
        thread::spawn(move || {
            for _sig in signals.forever() {
                if !shutting_down.swap(true, Ordering::SeqCst) {
                    let _ = tx.send(());
                }
            }
        });
    }

    let _ = rx.recv();
    let _ = bg_session.join();

    Ok(())
}
