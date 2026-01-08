use ctrlc;
use std::collections::{HashMap, HashSet};
use std::fs::FileType;
use std::io::{self, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
//use std::time::SystemTime;
use std::{ffi::c_void, ptr};
use tokio::runtime::Runtime;
use widestring::{U16CStr, U16CString};
use winfsp::filesystem::{
    DirBuffer, DirMarker, FileInfo, FileSecurity, FileSystemContext, OpenFileInfo,
};
use winfsp::host::{FileSystemHost, VolumeParams};
use winfsp::{FspError, Result as WinFspResult};
// API Windows per convertire SDDL -> SECURITY_DESCRIPTOR (self-relative)
use windows_sys::Win32::Foundation::{
    ERROR_ACCESS_DENIED, ERROR_ALREADY_EXISTS, ERROR_DIRECTORY, ERROR_FILE_NOT_FOUND,
    ERROR_INVALID_PARAMETER, ERROR_NOT_SAME_DEVICE, ERROR_NOT_SUPPORTED, HLOCAL, LocalFree,
};
use windows_sys::Win32::Security::Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW;
use windows_sys::Win32::Storage::FileSystem::{
    DELETE, FILE_APPEND_DATA, FILE_ATTRIBUTE_ARCHIVE, FILE_ATTRIBUTE_DIRECTORY,
    FILE_ATTRIBUTE_HIDDEN, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_READONLY, FILE_ATTRIBUTE_SYSTEM,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_READ,
    FILE_GENERIC_WRITE, FILE_WRITE_DATA,
};
//use windows_sys::Win32::System::IO::CREATE_DIRECTORY;
use winfsp::filesystem::DirInfo;
use winfsp_sys::FILE_FLAGS_AND_ATTRIBUTES;
use winfsp_sys::FspCleanupDelete;

use std::mem::{size_of, zeroed};
use std::ptr::addr_of_mut;
use std::slice;
use winfsp_sys::{FSP_FSCTL_DIR_INFO, FspFileSystemAddDirInfo};
//use std::cmp::Ordering;
use rust_socketio::{ClientBuilder, Payload};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct MyFileContext {
    pub ino: u64,
    pub temp_write: Option<TempWrite>, // Some se stiamo scrivendo, None se solo lettura
    pub delete_on_close: AtomicBool,
    pub allow_delete: bool,
    pub is_dir: bool,
    pub needs_truncate: AtomicBool,
    pub access_mask: u32,
}
//per la definizione fileAttr di file o directory
#[derive(Clone, Debug)]
enum NodeType {
    Directory,
    RegularFile,
}

use crate::file_api::{DirectoryEntry, FileApi};
const TTL: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct TempWrite {
    tem_path: PathBuf,
    size: u64,
}

// Definisco un FileAttr locale (simile a fuse::FileAttr).
#[derive(Clone, Debug)]
struct FileAttr {
    ino: u64,
    size: u64,
    blocks: u64,
    atime: SystemTime,
    mtime: SystemTime,
    ctime: SystemTime,
    crtime: SystemTime,
    kind: NodeType,
    perm: u16,
    nlink: u32,
    uid: u32,
    gid: u32,
    rdev: u32,
    blksize: u32,
    flags: u32,
}

#[derive(Clone)]
pub struct FsState {
    /// Mappa path -> inode
    pub ino_by_path: Arc<Mutex<HashMap<PathBuf, u64>>>,
    /// Mappa inode -> path
    pub path_by_ino: Arc<Mutex<HashMap<u64, PathBuf>>>,
    /// Cache degli attributi dei file
    pub attr_cache: Arc<Mutex<HashMap<PathBuf, FileAttr>>>,
    /// Cache delle directory (contenuto + timestamp)
    pub dir_cache: Arc<Mutex<HashMap<PathBuf, (Vec<DirectoryEntry>, SystemTime)>>>,
    /// File aperti in scrittura con temp file
    pub writes: Arc<Mutex<HashMap<u64, TempWrite>>>,
    /// Prossimo inode da allocare
    pub next_ino: Arc<Mutex<u64>>,
    /// TTL per la cache
    pub cache_ttl: Duration,
    /// Set di inode già cancellati (per evitare doppie cancellazioni)
    pub already_deleted: Arc<Mutex<HashSet<u64>>>,
}

impl FsState {
    pub fn new() -> Self {
        let mut ino_by_path = HashMap::new();
        let mut path_by_ino = HashMap::new();

        // Root sempre con inode 1
        ino_by_path.insert(PathBuf::from("."), 1);
        path_by_ino.insert(1, PathBuf::from("."));

        Self {
            ino_by_path: Arc::new(Mutex::new(ino_by_path)),
            path_by_ino: Arc::new(Mutex::new(path_by_ino)),
            attr_cache: Arc::new(Mutex::new(HashMap::new())),
            dir_cache: Arc::new(Mutex::new(HashMap::new())),
            writes: Arc::new(Mutex::new(HashMap::new())),
            next_ino: Arc::new(Mutex::new(2)),
            already_deleted: Arc::new(Mutex::new(HashSet::new())),
            cache_ttl: Duration::from_secs(300),
        }
    }

    // ---- PATH ↔ INODE ----

    pub fn ino_of(&self, path: &Path) -> Option<u64> {
        self.ino_by_path.lock().unwrap().get(path).cloned()
    }

    pub fn path_of(&self, ino: u64) -> Option<PathBuf> {
        self.path_by_ino.lock().unwrap().get(&ino).cloned()
    }

    pub fn allocate_ino(&self, path: &Path) -> u64 {
        if let Some(ino) = self.ino_by_path.lock().unwrap().get(path).cloned() {
            return ino;
        }

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

    pub fn clear_cache(&self, path: Option<&Path>) {
        match path {
            Some(p) => {
                self.remove_attr(p);
                self.remove_dir_cache(p);
            }
            None => {
                self.clear_all_cache();
            }
        }
    }

    // ---- WRITE TRACKING ----

    pub fn insert_write(&self, ino: u64, tw: TempWrite) {
        self.writes.lock().unwrap().insert(ino, tw);
    }

    pub fn get_write(&self, ino: u64) -> Option<TempWrite> {
        self.writes.lock().unwrap().get(&ino).cloned()
    }

    pub fn remove_write(&self, ino: u64) -> Option<TempWrite> {
        self.writes.lock().unwrap().remove(&ino)
    }

    // ---- DELETED TRACKING ----

    pub fn mark_deleted(&self, ino: u64) {
        self.already_deleted.lock().unwrap().insert(ino);
    }

    pub fn is_deleted(&self, ino: u64) -> bool {
        self.already_deleted.lock().unwrap().contains(&ino)
    }
}

struct RemoteFs {
    /// Stato condiviso (cache, mappature, ecc.)
    state: Arc<FsState>,
    /// Client API per comunicare con il backend
    api: FileApi,
    /// Runtime Tokio per operazioni async
    rt: Arc<Runtime>,
}

// Costanti WinAPI che non sempre sono re-esportate dal crate
//const FILE_WRITE_DATA: u32 = 0x0002;
const CREATE_DIRECTORY: u32 = 0x00000001; //TODO poi da provare ad usare un import
//const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x00000010;
const FSP_CLEANUP_DELETE: u32 = 0x20; //TODO vedere se si riesce ad importare
//const DELETE: u32 = 0x0001_0000; //TODO vedere se si riesce ad importare

impl RemoteFs {
    fn new(api: FileApi, rt: Arc<Runtime>) -> Self {
        Self {
            state: Arc::new(FsState::new()),
            api,
            rt,
        }
    }

    // Tutti i metodi ora usano self.state invece di accedere direttamente ai campi

    fn alloc_ino(&self, path: &Path) -> u64 {
        self.state.allocate_ino(path)
    }

    fn path_of(&self, ino: u64) -> Option<PathBuf> {
        self.state.path_of(ino)
    }

    pub fn init_cache(&self) {
        self.state.clear_all_cache();
    }

    pub fn get_attr_cache(&self, path: &Path) -> Option<FileAttr> {
        self.state.get_attr(path)
    }

    pub fn insert_attr_cache(&self, path: PathBuf, attr: FileAttr) {
        self.state.set_attr(&path, attr);
    }

    pub fn insert_dir_cache(&self, path: PathBuf, data: (Vec<DirectoryEntry>, SystemTime)) {
        self.state.set_dir_cache(&path, data);
    }

    pub fn get_dir_cache(&self, path: &Path) -> Option<(Vec<DirectoryEntry>, SystemTime)> {
        let cache_entry = self.state.get_dir_cache(path);
        if let Some((_, ts)) = &cache_entry {
            if !self.is_cache_valid(*ts) {
                return None;
            }
        }
        cache_entry
    }

    // Funzione che effettua l'aggiornamento della cache
    // Viene chiamata dopo operazioni di scrittura, creazione o cancellazione
    pub fn update_cache(&self, dir: &Path) -> anyhow::Result<()> {
        // 1) chiave canonica per il parent
        let rel = Self::rel_of(dir); // "." oppure "./a/b"
        let parent_key = PathBuf::from(rel.clone());

        // 2) backend refresh
        let list = self.rt.block_on(self.api.ls(&rel))?;

        // 3) aggiorna dir_cache con chiave canonica
        {
            let mut dircache = self.state.dir_cache.lock().unwrap();
            dircache.insert(parent_key.clone(), (list.clone(), SystemTime::now()));
        }

        // 4) aggiorna attr_cache in modo coerente e non aggressivo
        let mut attrcache = self.state.attr_cache.lock().unwrap();
        for de in &list {
            // sempre forma "./..." per i figli
            let child = if rel == "." || rel.is_empty() {
                PathBuf::from(format!("./{}", de.name))
            } else {
                let r = rel.trim_start_matches("./");
                PathBuf::from(format!("./{}/{}", r, de.name))
            };

            if !attrcache.contains_key(&child) {
                println!("[UPDATE CACHE] aggiornamento attr cache miss");
                let isdir = Self::is_dir(&de);
                let ty = if isdir {
                    NodeType::Directory
                } else {
                    NodeType::RegularFile
                };
                let perm = Self::parse_perm(&de.permissions);
                let size = if isdir { 0 } else { de.size.max(0) as u64 };
                let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm);
                println!(
                    "[INSERT ATTR CACHE] (path , attr) : ({:?}, {:?}) ",
                    child, attr
                );
                attrcache.insert(child.clone(), attr);
            }
        }

        Ok(())
    }

    pub fn is_cache_valid(&self, timestamp: SystemTime) -> bool {
        SystemTime::now().duration_since(timestamp).unwrap() < self.state.cache_ttl
    }

    fn sd_from_sddl(sddl: &str) -> anyhow::Result<Vec<u8>> {
        let sddl_u16 = U16CString::from_str(sddl)?;
        let mut sd_ptr: *mut c_void = ptr::null_mut();
        let mut sd_size: u32 = 0;
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl_u16.as_ptr(),
                1, // SDDL_REVISION_1
                (&mut sd_ptr as *mut *mut c_void).cast(),
                &mut sd_size as *mut u32,
            )
        };
        if ok == 0 {
            anyhow::bail!("ConvertStringSecurityDescriptorToSecurityDescriptorW failed");
        }
        let bytes =
            unsafe { std::slice::from_raw_parts(sd_ptr as *const u8, sd_size as usize).to_vec() };
        unsafe {
            LocalFree(sd_ptr as HLOCAL);
        }
        Ok(bytes)
    }
    /*
        fn alloc_ino(&self, path: &Path) -> u64 {
            if let Some(ino) = self.state.ino_by_path.lock().unwrap().get(path).cloned() {
                return ino;
            }
            let mut next_ino = self.state.next_ino.lock().unwrap();
            let ino = *next_ino;
            *next_ino += 1;
            self.state.ino_by_path
                .lock()
                .unwrap()
                .insert(path.to_path_buf(), ino);
            self.state.path_by_ino
                .lock()
                .unwrap()
                .insert(ino, path.to_path_buf());
            ino
        }

        fn path_of(&self, ino: u64) -> Option<PathBuf> {
            self.state.path_by_ino.lock().unwrap().get(&ino).cloned()
        }
    */
    fn rel_of(path: &Path) -> String {
        // to_string_lossy è sufficiente qui perché lavori con componenti ASCII del FS virtuale
        let mut s = path.to_string_lossy().replace('\\', "/");

        // Root o vuoto -> "."
        if s.is_empty() || s == "/" {
            return ".".to_string();
        }

        // Se è già relativo canonico che inizia con '.'
        if s.starts_with('.') {
            // Normalizza: elimina sequenze "././" in testa e "/." in coda
            while s.starts_with("././") {
                s = format!("./{}", &s[4..]);
            }
            if s == "./" || s == "./." {
                return ".".to_string();
            }
            if s.ends_with("/.") {
                s.truncate(s.len() - 2);
                if s.is_empty() || s == "./" {
                    return ".".to_string();
                }
            }
            return s;
        }

        // Se arriva come assoluto "/a/b" -> "./a/b"
        if s.starts_with('/') {
            s = s.trim_start_matches('/').to_string();
        }

        if s.is_empty() {
            ".".to_string()
        } else {
            format!("./{}", s)
        }
    }

    fn file_attr(
        &self,
        path: &Path,
        ty: NodeType,
        size: u64,
        mtime: Option<i64>,
        perm: u16,
    ) -> FileAttr {
        let now = SystemTime::now();
        let mtime_st = mtime
            .and_then(|sec| SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(sec as u64)))
            .unwrap_or(now);
        let uid = 0u32;
        let gid = 0u32;

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
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn parse_perm(permissions: &str) -> u16 {
        u16::from_str_radix(&permissions, 8).unwrap_or(0)
    }

    fn is_dir(de: &DirectoryEntry) -> bool {
        if de.is_dir == 1 {
            return true;
        }
        return false;
    }

    fn dir_entries(&self, dir: &Path) -> WinFspResult<Vec<(PathBuf, DirectoryEntry)>> {
        let rel = Self::rel_of(dir);
        //let rel=dir;

        println!("[DEBUG] dir_entries(): chiamata backend -> rel='{}'", rel);
        //1) prova cache directory

        if let Some((entries, ts)) = self
            .state
            .dir_cache
            .lock()
            .unwrap()
            .get(Path::new(&rel))
            .cloned()
        {
            if SystemTime::now()
                .duration_since(ts)
                .unwrap_or(Duration::ZERO)
                < self.state.cache_ttl
            {
                let mut out = Vec::with_capacity(entries.len());
                for de in entries {
                    let child_str = if rel == "." || rel.is_empty() {
                        format!("./{}", de.name)
                    } else {
                        let r = rel.trim_start_matches("./");
                        format!("/{}/{}", r, de.name)
                    };

                    let child = PathBuf::from(&child_str.replace('\\', "/"));
                    if self.get_attr_cache(&child).is_none() {
                        let is_dir = Self::is_dir(&de);
                        let ty = if is_dir {
                            NodeType::Directory
                        } else {
                            NodeType::RegularFile
                        };
                        let perm = Self::parse_perm(&de.permissions);
                        let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                        let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm);
                        self.insert_attr_cache(child.clone(), attr);
                    }
                    out.push((child, de));
                }
                return Ok(out);
            }
        }

        //2) se non trovata in cache chiamo il backend
        let list_res = self.rt.block_on(self.api.ls(&rel));

        match &list_res {
            //lo uso solo per il print
            Ok(list) => {
                println!("[DEBUG] dir_entries(): backend OK ({} entries)", list.len());
                for (i, de) in list.iter().enumerate() {
                    println!(
                        "  [{}] name='{}', perm='{}', size={}, mtime={}",
                        i, de.name, de.permissions, de.size, de.mtime
                    );
                }
            }
            Err(e) => {
                eprintln!("[DEBUG] dir_entries(): backend ERROR -> {}", e);
            }
        }

        let list = list_res.map_err(|e| {
            let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
            FspError::from(io_err)
        })?;

        //aggiorno cache directory
        self.insert_dir_cache(PathBuf::from(&rel), (list.clone(), SystemTime::now()));

        //costruisco out e pre-popolo attr_cache per i figli

        let mut out = Vec::with_capacity(list.len());

        // Normalizza il path base ma è già fatto da rel of provare con print
        /*
        let base_path = if dir == Path::new("/") || dir.to_string_lossy() == "/" {
            PathBuf::from("/")
        } else {
            // Assicurati che inizi con / e normalizza
            let s = dir.to_string_lossy();
            let normalized = if s.starts_with('/') {
                s.to_string()
            } else {
                format!("/{}", s.trim_start_matches("./"))
            };
            PathBuf::from(normalized)
        };*/
        println!(
            "[DIR_ENTRIES] path utilizzato caso no cache per file attr {}",
            rel
        );

        for de in list {
            let child_str = if rel == "." || rel.is_empty() {
                format!("./{}", de.name)
            } else {
                let r = rel.trim_start_matches("./");
                format!("/{}/{}", r, de.name)
            };

            let child = PathBuf::from(&child_str.replace('\\', "/"));
            let is_dir = Self::is_dir(&de);
            let ty = if is_dir {
                NodeType::Directory
            } else {
                NodeType::RegularFile
            };
            let perm = Self::parse_perm(&de.permissions);
            let size = if is_dir { 0 } else { de.size.max(0) as u64 };
            let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm);
            self.insert_attr_cache(child.clone(), attr);
            out.push((child, de))
        }

        Ok(out)
    }

    fn path_from_u16(&self, path: &U16CStr) -> String {
        // Converti U16CStr -> OsString -> String lossily
        let raw = path.to_os_string().to_string_lossy().to_string();
        println!("[DEBUG] path_from_u16 RAW input: '{}'", raw);

        let mut s = raw;

        // Normalizza separatori Windows -> Unix
        if s.contains('\\') {
            s = s.replace('\\', "/");
        }

        // Rimuovi eventuali doppie slash
        while s.contains("//") {
            s = s.replace("//", "/");
        }

        // Se la stringa è vuota -> root
        if s.is_empty() {
            return "/".to_string();
        }

        // Assicurati leading slash
        if !s.starts_with('/') {
            s = format!("/{}", s);
        }

        // Rimuovi trailing slash eccetto per root
        if s.len() > 1 && s.ends_with('/') {
            s.truncate(s.len() - 1);
        }

        s
    }

    fn get_temporary_path(&self, ino: u64) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("remotefs_tmp_{}.bin", ino));
        p
    }

    /// Helper: verifica se l'entry esiste nel backend (usa ls sul parent e cerca il nome)
    fn backend_entry_exists(&self, path: &str) -> bool {
        // rel è già nel formato corretto: "." o "./subdir/qualcosa"
        let rel_path = Path::new(path);

        // Calcola il parent relativo — se vuoto, usa "."
        let parent_rel = rel_path
            .parent()
            .map(|pp| pp.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());

        // Estrai il nome del file o directory
        let name = rel_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        println!(
            "[DEBUG] backend_entry_exists: rel='{}' -> parent_rel='{}' name='{}'",
            path, parent_rel, name
        );

        match self.rt.block_on(self.api.ls(&parent_rel)) {
            Ok(list) => {
                let exists = list.iter().any(|de| de.name == name);
                println!(
                    "[DEBUG] backend_entry_exists: parent='{}' found={} entries=[{}] exists={}",
                    parent_rel,
                    list.len(),
                    list.iter()
                        .map(|d| d.name.clone())
                        .collect::<Vec<_>>()
                        .join(", "),
                    exists
                );
                exists
            }
            Err(e) => {
                eprintln!("[DEBUG] backend_entry_exists: backend error: {}", e);
                false
            }
        }
    }

    fn nt_time_from_system_time(t: SystemTime) -> u64 {
        // NT epoch 1601-01-01 to Unix epoch 1970-01-01 in 100ns ticks
        const SECS_BETWEEN_EPOCHS: u64 = 11644473600;
        const HUNDRED_NS_PER_SEC: u64 = 10_000_000;

        match t.duration_since(UNIX_EPOCH) {
            Ok(dur) => {
                let secs = dur.as_secs().saturating_add(SECS_BETWEEN_EPOCHS);
                let sub_100ns = (dur.subsec_nanos() / 100) as u64;
                (secs.saturating_mul(HUNDRED_NS_PER_SEC)).saturating_add(sub_100ns)
            }
            Err(_) => 0,
        }
    }

    fn is_directory_from_permissions(p: &str) -> bool {
        p.chars().next().unwrap_or('-') == 'd'
    }

    fn evict_all_state_for(&self, path: &str) {
        //liberi la cache, mapping e temp write
        let path_buf = std::path::PathBuf::from(path);
        if let Some(ino) = self.state.ino_by_path.lock().unwrap().remove(&path_buf) {
            self.state.path_by_ino.lock().unwrap().remove(&ino);
            if let Some(tw) = self.state.writes.lock().unwrap().remove(&ino) {
                let _ = std::fs::remove_file(&tw.tem_path);
            }
        }
        self.state.attr_cache.lock().unwrap().remove(&path_buf);
    }

    fn can_delete(
        &self,
        file_context: &MyFileContext,
        //file_name: Option<&U16CStr>,
        rel: String,
    ) -> WinFspResult<()> {
        println!("[CAN_DELETE] enter");

        // Risolvi path
        /*  let path = if let Some(name) = file_name {
            let p = self.path_from_u16(name);
            println!("[CAN_DELETE] file_name provided -> path_from_u16 = {}", p);
            p
        } else {
            let p = self
                .path_of(file_context.ino)
                .map(|p| p.to_string_lossy().to_string())
                .ok_or(FspError::WIN32(
                    windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
                ))?;
            println!("[CAN_DELETE] no file_name, path_of(ino={}) = {}", file_context.ino, p);
            p
        };

        let rel = RemoteFs::rel_of(std::path::Path::new(&path));*/
        println!("[CAN_DELETE] rel = '{}'", rel);

        // Root non cancellabile
        if rel == "." {
            println!("[CAN_DELETE] rel='.' => deny delete: ERROR_ACCESS_DENIED");
            return Err(FspError::WIN32(
                windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED,
            ));
        }

        // Determina parent e name
        let parent_rel = std::path::Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let name_only = std::path::Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        println!(
            "[CAN_DELETE] parent_rel='{}', name_only='{}'",
            parent_rel, name_only
        );

        // Lista parent per trovare la dirent
        let list = match self.rt.block_on(self.api.ls(&parent_rel)) {
            Ok(v) => {
                println!(
                    "[CAN_DELETE] api.ls(parent='{}') ok: {} entries",
                    parent_rel,
                    v.len()
                );
                v
            }
            Err(e) => {
                println!(
                    "[CAN_DELETE] api.ls(parent='{}') ERR: {} -> map to Other",
                    parent_rel, e
                );
                return Err(FspError::from(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )));
            }
        };

        let de = match list.iter().find(|d| d.name == name_only) {
            Some(d) => {
                println!(
                    "[CAN_DELETE] found entry name='{}' is_dir={:?}",
                    d.name, d.is_dir
                );
                d
            }
            None => {
                println!(
                    "[CAN_DELETE] entry '{}' not found in parent '{}': ERROR_FILE_NOT_FOUND",
                    name_only, parent_rel
                );
                return Err(FspError::WIN32(
                    windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
                ));
            }
        };

        let is_dir = RemoteFs::is_dir(&de);
        println!("[CAN_DELETE] is_dir={}", is_dir);

        if is_dir {
            println!("[CAN_DELETE] directory case -> check emptiness for RemoveDirectory");
            // Directory: deve essere vuota
            let children = match self.rt.block_on(self.api.ls(&rel)) {
                Ok(v) => {
                    println!(
                        "[CAN_DELETE] api.ls(rel='{}') ok: {} children",
                        rel,
                        v.len()
                    );
                    v
                }
                Err(e) => {
                    println!(
                        "[CAN_DELETE] api.ls(rel='{}') ERR: {} -> map to Other",
                        rel, e
                    );
                    return Err(FspError::from(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )));
                }
            };
            if !children.is_empty() {
                println!("[CAN_DELETE] directory not empty -> ERROR_DIR_NOT_EMPTY");
                return Err(FspError::WIN32(
                    windows_sys::Win32::Foundation::ERROR_DIR_NOT_EMPTY,
                ));
            }
            println!("[CAN_DELETE] directory empty -> allow delete-on-close");
        } else {
            println!("[CAN_DELETE] file case -> allow delete-on-close");
        }

        // Se arrivi qui, WinFsp marcherà delete-on-close (per questa open) e la cancellazione dovrà avvenire in Cleanup con FspCleanupDelete.
        println!(
            "[CAN_DELETE] accept -> return Ok (WinFsp will signal FspCleanupDelete at Cleanup)"
        );
        Ok(())
    }

    //per trasformare il tempo da u64 a Systime
    fn filetime_to_systemtime(ft: u64) -> Option<SystemTime> {
        if ft == 0 {
            return None;
        }
        // FILETIME = 100ns ticks since 1601
        let duration = Duration::from_nanos(ft * 100);
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(11644473600) + duration)
    }

    // Helpers locali
    fn split_parent_name(rel: &str) -> (String, String) {
        let p = Path::new(rel);
        let parent = p
            .parent()
            .map(|x| x.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let name = p
            .file_name()
            .map(|x| x.to_string_lossy().to_string())
            .unwrap_or_default();
        (parent, name)
    }

    // Mappa errori backend (stub)
    fn map_backend_err<E: std::fmt::Display>(_: E) -> FspError {
        FspError::WIN32(ERROR_ACCESS_DENIED)
    }

    fn block_on<T>(rt: &tokio::runtime::Runtime, fut: impl std::future::Future<Output = T>) -> T {
        rt.block_on(fut)
    }
}

/// Normalizza path ricevuti dal WebSocket (gestisce path assoluti errati)
fn normalize_websocket_path(raw: &str) -> String {
    // Caso 1: Path già corretto "./file.txt"
    if raw.starts_with("./") || raw == "." {
        return raw.to_string();
    }

    // Caso 2: Path assoluto Windows "C:\Users\...\storage\file.txt"
    // Estrai solo la parte dopo "storage"
    if raw.contains("\\storage\\") || raw.contains("/storage/") {
        // Trova l'indice di "storage"
        let storage_idx = raw.rfind("storage").unwrap_or(0);

        if storage_idx > 0 {
            // Salta "storage\" o "storage/"
            let after_storage = &raw[storage_idx + 7..]; // 7 = len("storage")
            let clean = after_storage.trim_start_matches(['/', '\\']);

            if clean.is_empty() {
                return ".".to_string();
            }

            // Normalizza separatori e aggiungi prefisso
            let normalized = clean.replace('\\', "/");
            return format!("./{}", normalized);
        }
    }

    // Caso 3: Path relativo senza prefisso "file.txt" -> "./file.txt"
    if !raw.starts_with('/') && !raw.contains(":\\") {
        let normalized = raw.replace('\\', "/");
        return format!("./{}", normalized);
    }

    // Fallback: ritorna invariato con warning
    eprintln!("[WebSocket] WARNING: Unhandled path format: '{}'", raw);
    raw.to_string()
}

/// Estrae i metadati dal payload WebSocket
fn metadata_from_payload(payload: &Value) -> Option<(PathBuf, String, bool, u64, i64, u16)> {
    let raw_rel = payload["relPath"].as_str()?;

    // 🔴 NORMALIZZA: rimuovi path assoluto Windows se presente
    let rel = normalize_websocket_path(raw_rel);

    println!(
        "[WebSocket] metadata_from_payload: raw='{}' -> normalized='{}'",
        raw_rel, rel
    );
    let name = payload["name"]
        .as_str()
        .map(|s| s.to_string())
        .or_else(|| {
            Path::new(&rel)
                .file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        })?;

    let is_dir = payload["is_dir"].as_bool().unwrap_or(false);
    let size = payload["size"].as_u64().unwrap_or(0);
    let mtime = payload["mtime"].as_i64().unwrap_or(0);

    let perm_str = payload["permissions"].as_str().unwrap_or("644");
    let perm = u16::from_str_radix(perm_str, 8).unwrap_or(0o644);

    // Converti path relativo backend ("./a/b") in path canonico Windows (".\\a\\b")
    let abs_path = if rel == "." || rel.is_empty() {
        PathBuf::from(".")
    } else if rel.starts_with("./") {
        PathBuf::from(rel)
    } else {
        PathBuf::from(format!("./{}", rel))
    };

    Some((abs_path, name, is_dir, size, mtime, perm))
}

/// Avvia il listener WebSocket per ricevere notifiche di cambiamenti
pub fn start_websocket_listener(api_url: &str, fs_state: Arc<FsState>) {
    let ws_url = format!("{}/socket.io/", api_url.trim_end_matches('/'));

    tokio::spawn(async move {
        let fs_state_cloned = fs_state.clone();
        let ws_url_clone = ws_url.clone();

        tokio::task::spawn_blocking(move || {
            let client = ClientBuilder::new(ws_url_clone)
                .on("connect", |_, _| {
                    println!("[WebSocket] Connected!");
                })
                .on("fs_change", move |payload, _| match payload {
                    Payload::Text(values) => {
                        if values.len() < 1 {
                            eprintln!("[WebSocket] fs_change without data");
                            return;
                        }
                        let json_payload = &values[0];
                        handle_fs_change(json_payload, &fs_state_cloned);
                    }
                    _ => {
                        eprintln!("[WebSocket] Binary payload not supported");
                    }
                })
                .on("error", |err, _| {
                    eprintln!("[WebSocket] Error: {:?}", err);
                })
                .connect();

            if let Err(err) = client {
                eprintln!("[WebSocket] Connection failed: {:?}", err);
            }
        });
    });
}

/// Dispatcher principale per eventi fs_change
fn handle_fs_change(payload: &Value, fs_state: &FsState) {
    let op = payload["op"].as_str().unwrap_or("");

    println!("[WebSocket] Received fs_change: op={}", op);

    match op {
        "add" | "addDir" => {
            handle_created(payload, fs_state);
        }
        "write" | "change" => {
            handle_updated(payload, fs_state);
        }
        "unlink" | "unlinkDir" => {
            handle_deleted_event(payload, fs_state);
        }
        "rename" | "renameDir" => {
            handle_renamed_event(payload, fs_state);
        }
        _ => {
            println!("[WebSocket] Unknown op: {}", op);
        }
    }
}

/// Gestisce creazione di file/directory
fn handle_created(payload: &Value, st: &FsState) {
    let Some((abs, name, is_dir, size, mtime, perm)) = metadata_from_payload(payload) else {
        eprintln!("[WebSocket] handle_created: invalid metadata");
        return;
    };

    println!("[WebSocket] CREATE: path={:?} is_dir={}", abs, is_dir);

    // Aggiorna cache con nuovi dati
    let _ino = update_cache_from_metadata(st, &abs, &name, is_dir, size, mtime, perm);

    // Invalida parent directory (forza rilettura al prossimo accesso)
    if let Some(parent) = abs.parent() {
        st.remove_dir_cache(parent);
        println!("[WebSocket] Invalidated parent dir cache: {:?}", parent);
    }

    // IMPORTANTE: Windows/Explorer leggerà i nuovi dati al prossimo readdir/getattr
}

/// Gestisce aggiornamento di file
fn handle_updated(payload: &Value, st: &FsState) {
    let Some((abs, name, is_dir, size, mtime, perm)) = metadata_from_payload(payload) else {
        eprintln!("[WebSocket] handle_updated: invalid metadata");
        return;
    };

    println!("[WebSocket] UPDATE: path={:?} size={}", abs, size);

    // Aggiorna attributi in cache
    update_cache_from_metadata(st, &abs, &name, is_dir, size, mtime, perm);

    // Invalida parent per forzare refresh visivo in Explorer
    if let Some(parent) = abs.parent() {
        st.remove_dir_cache(parent);
    }
}

/// Gestisce cancellazione
fn handle_deleted_event(payload: &Value, st: &FsState) {
    if let Some(rel) = payload["relPath"].as_str() {
        let abs = if rel == "." || rel.is_empty() {
            PathBuf::from(".")
        } else if rel.starts_with("./") {
            PathBuf::from(rel)
        } else {
            PathBuf::from(format!("./{}", rel))
        };

        println!("[WebSocket] DELETE: path={:?}", abs);

        handle_deleted_path(&abs, st);
    } else {
        eprintln!("[WebSocket] handle_deleted_event: missing relPath");
    }
}

/// Rimuove tutti gli stati per un path cancellato
fn handle_deleted_path(abs: &Path, st: &FsState) {
    // Marca come cancellato PRIMA di rimuovere
    if let Some(ino) = st.ino_of(abs) {
        st.mark_deleted(ino);
        println!("[WebSocket] Marked inode {} as deleted", ino);
    }

    // Rimuovi mapping e cache
    st.remove_path(abs);
    st.remove_attr(abs);

    // Invalida parent directory (critico per Explorer)
    if let Some(parent) = abs.parent() {
        st.remove_dir_cache(parent);
        println!(
            "[WebSocket] Invalidated parent dir cache after delete: {:?}",
            parent
        );
    }
}

/// Gestisce rename
fn handle_renamed_event(payload: &Value, st: &FsState) {
    let Some(old_rel) = payload["oldPath"].as_str() else {
        eprintln!("[WebSocket] handle_renamed_event: missing oldPath");
        return;
    };
    let Some(new_rel) = payload["newPath"].as_str() else {
        eprintln!("[WebSocket] handle_renamed_event: missing newPath");
        return;
    };

    // Converti entrambi i path nel formato canonico
    let old_abs = if old_rel.starts_with("./") {
        PathBuf::from(old_rel)
    } else {
        PathBuf::from(format!("./{}", old_rel))
    };

    let new_abs = if new_rel.starts_with("./") {
        PathBuf::from(new_rel)
    } else {
        PathBuf::from(format!("./{}", new_rel))
    };

    println!("[WebSocket] RENAME: {:?} -> {:?}", old_abs, new_abs);

    // Mantieni l'inode se esiste, altrimenti allocane uno nuovo
    let ino = if let Some(ino) = st.ino_of(&old_abs) {
        st.remove_path(&old_abs);
        st.insert_path_mapping(&new_abs, ino);
        ino
    } else {
        st.allocate_ino(&new_abs)
    };

    // Aggiorna metadata del nuovo path
    if let Some((_, name, is_dir, size, mtime, perm)) = metadata_from_payload(payload) {
        update_cache_from_metadata(st, &new_abs, &name, is_dir, size, mtime, perm);
    }

    // Rimuovi cache del vecchio path
    st.remove_attr(&old_abs);

    // Invalida ENTRAMBI i parent (potrebbero essere diversi in caso di move)
    if let Some(old_parent) = old_abs.parent() {
        st.remove_dir_cache(old_parent);
        println!("[WebSocket] Invalidated old parent: {:?}", old_parent);
    }
    if let Some(new_parent) = new_abs.parent() {
        st.remove_dir_cache(new_parent);
        println!("[WebSocket] Invalidated new parent: {:?}", new_parent);
    }
}

/// Aggiorna le cache da metadata ricevuti via WebSocket
pub fn update_cache_from_metadata(
    st: &FsState,
    abs: &Path,
    name: &str,
    is_dir: bool,
    size: u64,
    mtime: i64,
    perm: u16,
) -> u64 {
    let kind = if is_dir {
        NodeType::Directory
    } else {
        NodeType::RegularFile
    };

    let ino = match st.ino_of(abs) {
        Some(i) => i,
        None => st.allocate_ino(abs),
    };

    let blocks = if size == 0 { 0 } else { (size + 511) / 512 };
    let mtime_st = UNIX_EPOCH + Duration::from_secs(mtime.max(0) as u64);

    let attr = FileAttr {
        ino,
        size,
        blocks,
        atime: mtime_st,
        mtime: mtime_st,
        ctime: mtime_st,
        crtime: mtime_st,
        kind,
        perm,
        nlink: if is_dir { 2 } else { 1 },
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    };

    st.set_attr(abs, attr);

    // Invalida parent directory cache
    if let Some(parent) = abs.parent() {
        st.remove_dir_cache(parent);
    }

    ino
}

impl FileSystemContext for RemoteFs {
    type FileContext = MyFileContext;

    fn get_security_by_name(
        &self,
        name: &U16CStr,
        buf: Option<&mut [c_void]>,
        _f: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> WinFspResult<FileSecurity> {
        let path_abs = self.path_from_u16(name);
        let rel = RemoteFs::rel_of(std::path::Path::new(&path_abs));
        let is_root = rel == ".";

        println!("[GET_SECURITY_BY_NAME] path='{}' rel='{}'", path_abs, rel);

        // 1) Prepara SD valido (usa sempre lo stesso SDDL per coerenza)
        let sd_bytes = RemoteFs::sd_from_sddl("O:BAG:BAD:(A;;FA;;;WD)(A;;FA;;;BA)(A;;FA;;;SY)")
            .unwrap_or_else(|_| {
                eprintln!("[GET_SECURITY_BY_NAME] WARN: sd_from_sddl failed, using empty SD");
                Vec::new()
            });

        let required = sd_bytes.len();
        println!("[GET_SECURITY_BY_NAME] SD size={} bytes", required);

        // 2) ROOT: esiste sempre
        if is_root {
            // Copia SD nel buffer se fornito e capiente
            if let Some(buff) = buf {
                if buff.len() >= required && required > 0 {
                    unsafe {
                        let dst = buff.as_mut_ptr() as *mut u8;
                        std::ptr::copy_nonoverlapping(sd_bytes.as_ptr(), dst, required);
                    }
                    println!("[GET_SECURITY_BY_NAME] SD copied to buffer (root)");
                } else if buff.len() < required {
                    println!(
                        "[GET_SECURITY_BY_NAME] Buffer too small: {} < {}",
                        buff.len(),
                        required
                    );
                }
            }

            return Ok(FileSecurity {
                reparse: false,
                attributes: FILE_ATTRIBUTE_DIRECTORY,
                sz_security_descriptor: required as u64,
            });
        }

        // 3) Determina parent e nome
        let parent_rel = Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());

        let name_only = Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        let parent_path = PathBuf::from(&parent_rel);

        println!(
            "[GET_SECURITY_BY_NAME] parent='{}' name='{}' parent_key='{}'",
            parent_rel,
            name_only,
            parent_path.display()
        );

        // 4) Cerca entry nel parent (cache-aware)
        let list = match self.dir_entries(&parent_path) {
            Ok(v) => {
                println!("[GET_SECURITY_BY_NAME] dir_entries OK: {} entries", v.len());
                v
            }
            Err(e) => {
                eprintln!("[GET_SECURITY_BY_NAME] dir_entries FAILED: {}", e);
                return Err(e);
            }
        };

        // 5) Cerca il file specifico
        if let Some((child_path, de)) = list.iter().find(|(_, d)| d.name == name_only) {
            let is_dir = RemoteFs::is_dir(&de);
            let attrs = if is_dir {
                FILE_ATTRIBUTE_DIRECTORY
            } else {
                FILE_ATTRIBUTE_NORMAL
            };

            // Alloca ino (idempotente)
            let _ = self.alloc_ino(std::path::Path::new(&path_abs));

            // Copia SD nel buffer se fornito e capiente
            if let Some(buff) = buf {
                if buff.len() >= required && required > 0 {
                    unsafe {
                        let dst = buff.as_mut_ptr() as *mut u8;
                        std::ptr::copy_nonoverlapping(sd_bytes.as_ptr(), dst, required);
                    }
                    println!("[GET_SECURITY_BY_NAME] SD copied to buffer");
                } else if buff.len() < required {
                    println!(
                        "[GET_SECURITY_BY_NAME] Buffer too small: {} < {}",
                        buff.len(),
                        required
                    );
                }
            }

            println!(
                "[GET_SECURITY_BY_NAME] FOUND '{}' is_dir={} attrs={:#x} sd_len={}",
                child_path.display(),
                is_dir,
                attrs,
                required
            );

            return Ok(FileSecurity {
                reparse: false,
                attributes: attrs,
                sz_security_descriptor: required as u64,
            });
        }

        // 6) Non trovato
        eprintln!(
            "[GET_SECURITY_BY_NAME] NOT FOUND '{}' in parent '{}'",
            name_only, parent_rel
        );

        Err(FspError::WIN32(ERROR_FILE_NOT_FOUND))
    }

    fn get_security(
        &self,
        context: &Self::FileContext,
        security_descriptor: Option<&mut [c_void]>,
    ) -> WinFspResult<u64> {
        println!("[GET_SECURITY] ino={}", context.ino);

        // Usa LO STESSO SDDL di get_security_by_name per coerenza
        let sd_bytes = Self::sd_from_sddl("O:BAG:BAD:(A;;FA;;;WD)(A;;FA;;;BA)(A;;FA;;;SY)")
            .unwrap_or_else(|_| {
                eprintln!("[GET_SECURITY] WARN: sd_from_sddl failed, using empty SD");
                Vec::new()
            });

        let sd_len = sd_bytes.len();
        println!("[GET_SECURITY] SD size={} bytes", sd_len);

        // Se chiamante chiede solo la size
        if security_descriptor.is_none() {
            return Ok(sd_len as u64);
        }

        // Copia nel buffer se capiente
        let buf_void = security_descriptor.unwrap();
        let buf_len = buf_void.len();

        if buf_len < sd_len {
            println!("[GET_SECURITY] Buffer too small: {} < {}", buf_len, sd_len);
            return Err(FspError::WIN32(
                windows_sys::Win32::Foundation::ERROR_INSUFFICIENT_BUFFER,
            ));
        }

        let dst_u8: &mut [u8] =
            unsafe { slice::from_raw_parts_mut(buf_void.as_mut_ptr() as *mut u8, buf_len) };

        dst_u8[..sd_len].copy_from_slice(&sd_bytes);
        println!("[GET_SECURITY] SD copied to buffer");

        Ok(sd_len as u64)
    }

    fn get_file_info(&self, context: &MyFileContext, file_info: &mut FileInfo) -> WinFspResult<()> {
        println!(
            "[GET_FILE_INFO] start ino={} is_dir={}",
            context.ino, context.is_dir
        );

        // 1) Path canonico dal mapping ino -> path -> rel "./..."
        let path = match self.path_of(context.ino) {
            Some(p) => p,
            None => {
                println!(
                    "[GET_FILE_INFO] ERROR: ino={} non mappato -> FILE_NOT_FOUND",
                    context.ino
                );
                return Err(FspError::WIN32(
                    windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
                ));
            }
        };
        let rel = RemoteFs::rel_of(&path);
        println!(
            "[GET_FILE_INFO] path_abs='{}' rel='{}'",
            path.display(),
            rel
        );

        // 2) Directory
        if context.is_dir {
            file_info.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
            file_info.file_size = 0;
            println!(
                "[GET_FILE_INFO] dir path='{}' set attrs=DIR size=0 (cache lookup)",
                rel
            );

            if let Some(attr) = self.get_attr_cache(&PathBuf::from(&rel)) {
                file_info.creation_time = RemoteFs::nt_time_from_system_time(attr.crtime);
                file_info.last_access_time = RemoteFs::nt_time_from_system_time(attr.atime);
                file_info.last_write_time = RemoteFs::nt_time_from_system_time(attr.mtime);
                file_info.change_time = RemoteFs::nt_time_from_system_time(attr.ctime);

                println!(
                    "[GET_FILE_INFO] dir cache hit: cr={:#x} at={:#x} wt={:#x} ct={:#x}",
                    file_info.creation_time,
                    file_info.last_access_time,
                    file_info.last_write_time,
                    file_info.change_time
                );
                println!("[GET_FILE_INFO] done (dir, cache) OK");
                return Ok(());
            }

            // Fallback: risolvi parent e nome, poi cerca in dir_entries(parent)
            let parent_rel = Path::new(&rel)
                .parent()
                .map(|p| p.to_string_lossy().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| ".".to_string());
            let name_only = Path::new(&rel)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            println!(
                "[GET_FILE_INFO] dir cache miss -> lookup parent_rel='{}' name='{}'",
                parent_rel, name_only
            );

            let parent_key = PathBuf::from(parent_rel);
            let entries = self.dir_entries(&parent_key)?;
            println!(
                "[GET_FILE_INFO] dir parent entries loaded: count={}",
                entries.len()
            );

            if let Some((_, de)) = entries.iter().find(|(_, d)| d.name == name_only) {
                let t = std::time::UNIX_EPOCH
                    .checked_add(std::time::Duration::from_secs(de.mtime.max(0) as u64))
                    .unwrap_or_else(std::time::SystemTime::now);
                let nt = RemoteFs::nt_time_from_system_time(t);
                file_info.creation_time = nt;
                file_info.last_access_time = nt;
                file_info.last_write_time = nt;
                file_info.change_time = nt;

                println!("[GET_FILE_INFO] dir fallback hit: mt={:?} nt={:#x}", t, nt);
                println!("[GET_FILE_INFO] done (dir, fallback) OK");
                return Ok(());
            }

            // Ultimo fallback: timestamps 0
            file_info.creation_time = 0;
            file_info.last_access_time = 0;
            file_info.last_write_time = 0;
            file_info.change_time = 0;
            println!(
                "[GET_FILE_INFO] dir not found in parent entries -> timestamps=0 (graceful OK)"
            );
            println!("[GET_FILE_INFO] done (dir, zeros) OK");
            return Ok(());
        }

        // 3) File: prova attrcache
        if let Some(attr) = self.get_attr_cache(&PathBuf::from(&rel)) {
            let readonly = (attr.perm & 0o222) == 0;
            file_info.file_attributes = if readonly {
                FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_READONLY
            } else {
                FILE_ATTRIBUTE_NORMAL
            };
            file_info.file_size = attr.size;
            file_info.creation_time = RemoteFs::nt_time_from_system_time(attr.crtime);
            file_info.last_access_time = RemoteFs::nt_time_from_system_time(attr.atime);
            file_info.last_write_time = RemoteFs::nt_time_from_system_time(attr.mtime);
            file_info.change_time = RemoteFs::nt_time_from_system_time(attr.ctime);

            println!(
                "[GET_FILE_INFO] file cache hit: attrs={:#x} size={} cr={:#x} at={:#x} wt={:#x} ct={:#x} perm={:#o} readonly={}",
                file_info.file_attributes,
                file_info.file_size,
                file_info.creation_time,
                file_info.last_access_time,
                file_info.last_write_time,
                file_info.change_time,
                attr.perm,
                readonly
            );
            println!("[GET_FILE_INFO] done (file, cache) OK");
            return Ok(());
        }
        println!("[GET_FILE_INFO] file cache miss for '{}'", rel);

        // 4) Fallback file: cerca DirectoryEntry via parent
        let parent_rel = Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let name_only = Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        println!(
            "[GET_FILE_INFO] file lookup parent_rel='{}' name='{}'",
            parent_rel, name_only
        );

        let parent_key = PathBuf::from(parent_rel);
        let entries = self.dir_entries(&parent_key)?;
        println!(
            "[GET_FILE_INFO] file parent entries loaded: count={}",
            entries.len()
        );

        if let Some((child_path, de)) = entries.into_iter().find(|(_, d)| d.name == name_only) {
            let isdir = RemoteFs::is_dir(&de);
            let perm = RemoteFs::parse_perm(&de.permissions);

            if isdir {
                file_info.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
                file_info.file_size = 0;
                println!(
                    "[GET_FILE_INFO] backend says DIR (context said file): force attrs=DIR size=0"
                );
            } else {
                let readonly = (perm & 0o222) == 0;
                file_info.file_attributes = if readonly {
                    FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_READONLY
                } else {
                    FILE_ATTRIBUTE_NORMAL
                };
                file_info.file_size = de.size.max(0) as u64;
                println!(
                    "[GET_FILE_INFO] backend file: attrs={:#x} size={} perm={:#o} readonly={}",
                    file_info.file_attributes, file_info.file_size, perm, readonly
                );
            }

            let t = std::time::UNIX_EPOCH
                .checked_add(std::time::Duration::from_secs(de.mtime.max(0) as u64))
                .unwrap_or_else(std::time::SystemTime::now);
            let nt = RemoteFs::nt_time_from_system_time(t);
            file_info.creation_time = nt;
            file_info.last_access_time = nt;
            file_info.last_write_time = nt;
            file_info.change_time = nt;
            println!(
                "[GET_FILE_INFO] timestamps from backend: mt={:?} nt={:#x}",
                t, nt
            );

            // Aggiorna attrcache
            let ty = if isdir {
                NodeType::Directory
            } else {
                NodeType::RegularFile
            };
            let size = if isdir { 0 } else { de.size.max(0) as u64 };
            let attr = self.file_attr(&child_path, ty, size, Some(de.mtime), perm);
            self.insert_attr_cache(child_path.clone(), attr);
            println!(
                "[GET_FILE_INFO] attrcache updated for '{}'",
                child_path.display()
            );

            println!("[GET_FILE_INFO] done (file, fallback) OK");
            return Ok(());
        }

        // 5) Non trovato
        println!(
            "[GET_FILE_INFO] ERROR: entry '{}' non trovata tra i figli -> FILE_NOT_FOUND",
            rel
        );
        Err(FspError::WIN32(
            windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
        ))
    }

    fn rename(
        &self,
        context: &MyFileContext,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
        replace_if_exists: bool,
    ) -> WinFspResult<()> {
        // 1) Canonicalizza path
        let src_abs = self.path_from_u16(file_name);
        let dst_abs = self.path_from_u16(new_file_name);
        let src_rel = RemoteFs::rel_of(std::path::Path::new(&src_abs));
        let dst_rel = RemoteFs::rel_of(std::path::Path::new(&dst_abs));
        println!(
            "[RENAME] start ino={} is_dir={} src='{}' -> dst='{}' replace={}",
            context.ino, context.is_dir, src_rel, dst_rel, replace_if_exists
        );

        // Root non rinominabile
        if src_rel == "." {
            println!("[RENAME] denied: source is root");
            return Err(FspError::WIN32(ERROR_ACCESS_DENIED));
        }
        if let Some(attr) = self.get_attr_cache(&PathBuf::from(&src_rel)) {
            if (attr.perm & 0o222) == 0 {
                println!(
                    "[RENAME] denied by perm for {} perm={:#o}",
                    src_rel, attr.perm
                );
                return Err(FspError::WIN32(ERROR_ACCESS_DENIED));
            }
        }

        // 2) Parent/nome canonici
        let (src_parent_rel, src_name) = RemoteFs::split_parent_name(&src_rel);
        let (dst_parent_rel, dst_name) = RemoteFs::split_parent_name(&dst_rel);
        let src_parent_key = std::path::PathBuf::from(&src_parent_rel);
        let dst_parent_key = std::path::PathBuf::from(&dst_parent_rel);

        // 3) Liste parent (cache-aware)
        let src_list = self.dir_entries(&src_parent_key).map_err(|e| {
            eprintln!("[RENAME] dir_entries('{}') failed: {}", src_parent_rel, e);
            e
        })?;
        let dst_list = if src_parent_rel == dst_parent_rel {
            src_list.clone()
        } else {
            self.dir_entries(&dst_parent_key).map_err(|e| {
                eprintln!("[RENAME] dir_entries('{}') failed: {}", dst_parent_rel, e);
                e
            })?
        };

        // 4) Sorgente deve esistere
        let (src_child_path, src_de) = match src_list.iter().find(|(_, d)| d.name == src_name) {
            Some((p, d)) => (p.clone(), d.clone()),
            None => {
                eprintln!(
                    "[RENAME] source '{}' not found in '{}'",
                    src_name, src_parent_rel
                );
                return Err(FspError::WIN32(
                    windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
                ));
            }
        };
        let src_is_dir = RemoteFs::is_dir(&src_de);

        // 5) Gestisci destinazione esistente e replace_if_exists
        if let Some((_, dst_de)) = dst_list.iter().find(|(_, d)| d.name == dst_name) {
            let dst_is_dir = RemoteFs::is_dir(&dst_de);
            if src_is_dir != dst_is_dir {
                eprintln!(
                    "[RENAME] type mismatch: src_is_dir={} dst_is_dir={}",
                    src_is_dir, dst_is_dir
                );
                return Err(FspError::WIN32(
                    windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED,
                ));
            }
            if !replace_if_exists {
                eprintln!("[RENAME] destination exists and replace_if_exists=false");
                return Err(FspError::WIN32(
                    windows_sys::Win32::Foundation::ERROR_ALREADY_EXISTS,
                ));
            }
            if dst_is_dir {
                eprintln!("[RENAME] replace directory not supported");
                return Err(FspError::WIN32(
                    windows_sys::Win32::Foundation::ERROR_NOT_SUPPORTED,
                ));
            }
            // Emula REPLACE_EXISTING se hai delete
            if let Err(e) = self.rt.block_on(self.api.delete(&dst_rel)) {
                eprintln!("[RENAME] pre-delete failed for '{}': {}", dst_rel, e);
                return Err(FspError::WIN32(
                    windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED,
                ));
            }
        }

        // 6) Backend rename (async PATCH /files/rename?oldRelPath=&newRelPath=)
        if let Err(e) = self.rt.block_on(self.api.rename(&src_rel, &dst_rel)) {
            eprintln!(
                "[RENAME] backend rename failed: {} -> {} err={}",
                src_rel, dst_rel, e
            );
            return Err(FspError::WIN32(
                windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED,
            ));
        }

        // 7) Aggiorna cache in modo coerente con le tue API
        // Evict state vecchio path (attrcache/dircache/inode mapping/pendenze write)
        self.evict_all_state_for(&src_rel);

        // Hard refresh dei parent coinvolti (usa le tue primitive)
        // - Se hai update_cache(Path), usala per ricaricare da backend e ripopolare dircache/attrcache.
        // - Se no, chiama ls e insert_dir_cache come già fai altrove.

        // Aggiorna cache parent sorgente e destinazione
        if let Err(e) = self.update_cache(&src_parent_key) {
            eprintln!("[RENAME] update_cache('{}') failed: {}", src_parent_rel, e);
            // continua comunque
        }
        if src_parent_rel != dst_parent_rel {
            if let Err(e) = self.update_cache(&dst_parent_key) {
                eprintln!("[RENAME] update_cache('{}') failed: {}", dst_parent_rel, e);
            }
        }

        // 8) Aggiorna mappa ino->path se il context punta proprio all’oggetto rinominato
        if let Some(cur) = self.path_of(context.ino) {
            if RemoteFs::rel_of(&cur) == src_rel {
                // path_by_ino modificabile con le tue strutture
                if let Ok(mut byino) = self.state.path_by_ino.lock() {
                    byino.insert(context.ino, PathBuf::from(dst_abs.clone()));
                }
                if let Ok(mut bypath) = self.state.ino_by_path.lock() {
                    bypath.remove(&cur);
                    bypath.insert(std::path::PathBuf::from(&dst_abs), context.ino);
                }
            }
        }

        println!(
            "[RENAME] done: '{}' -> '{}' (replace_if_exists={})",
            src_rel, dst_rel, replace_if_exists
        );
        Ok(())
    }

    fn get_stream_info(
        &self,
        context: &Self::FileContext,
        buffer: &mut [u8],
    ) -> Result<u32, FspError> {
        // Non supportiamo ADS: ritorniamo vuoto (0 bytes scritti)
        Ok(0)
    }

    fn get_volume_info(
        &self,
        out_volume_info: &mut winfsp::filesystem::VolumeInfo,
    ) -> WinFspResult<()> {
        println!("[GET_VOLUME_INFO] start");

        // Chiama il backend per ottenere le statistiche (probabilmente hai un endpoint /stats o /df)
        let stats = self.rt.block_on(self.api.statfs()).map_err(|e| {
            eprintln!("[GET_VOLUME_INFO] statfs backend failed: {}", e);
            FspError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        // Popola VolumeInfo con i dati dal backend
        // Assicurati che il tuo FileApi::statfs() ritorni una struct con questi campi
        // (vedi il tuo file_api.rs: StatsResponse ha bsize, blocks, bfree, bavail, files)

        out_volume_info.total_size = stats.blocks * stats.bsize; // blocchi totali * size
        out_volume_info.free_size = stats.bfree * stats.bsize; // blocchi liberi * size
        println!(
            "[GET_VOLUME_INFO] total={} free={} (in bytes)",
            out_volume_info.total_size, out_volume_info.free_size,
        );

        Ok(())
    }

    fn open(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        mut granted_access: u32,
        open_info: &mut OpenFileInfo,
    ) -> WinFspResult<Self::FileContext> {
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_READONLY,
        };

        println!("[OPEN] .0 entry");

        // 1) Path Win32 -> rel canonico per cache/backend
        let path = self.path_from_u16(file_name);
        let rel = RemoteFs::rel_of(std::path::Path::new(&path));
        println!("[OPEN] .1 path_from_u16 -> path='{}' rel='{}'", path, rel);

        // DEBUG: stampa tutti i permission flag ricevuti
        const GENERIC_READ: u32 = 0x80000000;
        const GENERIC_WRITE: u32 = 0x40000000;
        const GENERIC_EXECUTE: u32 = 0x20000000;
        const READ_CONTROL: u32 = 0x00020000;
        const WRITE_DAC: u32 = 0x00040000;
        const SYNCHRONIZE: u32 = 0x00100000;
        // Windows file access: leggere SOLO attributi (Explorer/WinAPI spesso usa questo)
        const FILE_READ_ATTRIBUTES: u32 = 0x00000080;
        const FILE_READ_DATA: u32 = 0x00000001;
        const FILE_DELETE_CHILD: u32 = 0x00000040;

        println!("[OPEN] .2 granted_access=0x{:08X}", granted_access);
        println!(
            "[OPEN] .2 flags GENERIC_READ={} GENERIC_WRITE={} FILE_READ_ATTRIBUTES={}",
            (granted_access & GENERIC_READ) != 0,
            (granted_access & GENERIC_WRITE) != 0,
            (granted_access & FILE_READ_ATTRIBUTES) != 0
        );
        /*     //forza a chiamare la read
        if (granted_access & FILE_READ_ATTRIBUTES) != 0
            && (granted_access & FILE_READ_DATA) == 0
            && (granted_access & GENERIC_READ) == 0
        {
            println!("[OPEN] upgrading FILE_READ_ATTRIBUTES -> FILE_READ_DATA");
            granted_access |= FILE_READ_DATA | GENERIC_READ;
        }*/

        println!("[OPEN] .2 granted_access=0x{:08X}", granted_access);
        println!("[OPEN] .2 granted_access binary: {:032b}", granted_access);
        println!(
            "[OPEN] DELETE bit (0x{:08X}): {}",
            DELETE,
            (granted_access & DELETE) != 0
        );

        let wants_delete = (granted_access & DELETE) != 0;
        let wants_write =
            (granted_access & FILE_WRITE_DATA) != 0 || (granted_access & GENERIC_WRITE) != 0;
        // Accetta FILE_READ_DATA, GENERIC_READ
        let wants_read =
            (granted_access & FILE_READ_DATA) != 0 || (granted_access & GENERIC_READ) != 0;
        //Vuole solo leggere gli attributi (Explorer usa questo per "proprietà" senza aprire il file)
        let wants_read_attributes = (granted_access & FILE_READ_ATTRIBUTES) != 0;
        let has_delete_child = granted_access & FILE_DELETE_CHILD != 0;

        println!(
            "[OPEN] .3 wants_delete={} wants_write={} wants_read={}  wants_read_attributes={} has_delete_child={}",
            wants_delete, wants_write, wants_read, wants_read_attributes, has_delete_child
        );

        // 2) Root
        if rel == "." {
            println!("[OPEN] .4 root case -> returning dir context");
            let fi = open_info.as_mut();
            fi.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
            fi.file_size = 0;
            // alloc_ino su chiave canonica della root, NON su "/"
            let ino = self.alloc_ino(std::path::Path::new(".")); // FIX
            return Ok(MyFileContext {
                ino,
                is_dir: true,
                allow_delete: wants_delete,
                delete_on_close: AtomicBool::new(false),
                temp_write: None,
                needs_truncate: AtomicBool::new(false),
                access_mask: granted_access,
            });
        }

        // 3) Parent/name canonici
        let parent_rel = std::path::Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let name_only = std::path::Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        let parent_key = std::path::PathBuf::from(&parent_rel);

        println!(
            "[OPEN] .5 parent_rel='{}' name_only='{}' parent_key='{:?}'",
            parent_rel, name_only, parent_key
        );
        println!("[OPEN] .6 calling dir_entries(parent)");

        let entries = match self.dir_entries(&parent_key) {
            Ok(v) => {
                println!("[OPEN] .6 dir_entries OK count={}", v.len());
                v
            }
            Err(e) => {
                eprintln!(
                    "[OPEN] .E dir_entries FAILED for parent='{}' err={}",
                    parent_rel, e
                );
                return Err(e);
            }
        }; // cache-aware su chiavi canoniche

        // 4) Trova figlio: child_path è canonico ("./nome")
        let target_name = std::ffi::OsStr::new(&name_only);
        let (child_path, de) = entries
            .clone()
            .into_iter()
            .find(|(_, d)| d.name == name_only) // ← Rimuovi il confronto su path.file_name()
            .ok_or_else(|| {
                eprintln!(
                    "[OPEN] .E child not found: '{}' in parent '{}'",
                    name_only, parent_rel
                );
                eprintln!("[OPEN] .E Searched among {} entries", entries.len());
                FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND)
            })?;

        println!(
            "[OPEN] .7 found child child_path='{}' backend_name='{}'",
            child_path.display(),
            de.name
        );

        let is_dir = RemoteFs::is_dir(&de);
        println!("[OPEN] .8 is_dir={}", is_dir);

        // 5) alloc_ino sulla chiave canonica del figlio (NON su 'path' Win32)
        let ino = self.alloc_ino(&child_path); // FIX
        println!("[OPEN] .9 alloc_ino -> ino={}", ino);

        // 6) Compila OpenFileInfo coerentemente
        let fi = open_info.as_mut();
        if is_dir {
            println!("[OPEN] .10 returning dir context for child");

            // 🌟 PASSO CRITICO: Recupera gli attributi completi per la directory
            if let Some(attr) = self.get_attr_cache(&child_path) {
                fi.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
                fi.file_size = 0;

                // ✅ IL FIX: Mappa i timestamp dalla cache a OpenFileInfo
                fi.creation_time = RemoteFs::nt_time_from_system_time(attr.crtime);
                fi.last_access_time = RemoteFs::nt_time_from_system_time(attr.atime);
                fi.last_write_time = RemoteFs::nt_time_from_system_time(attr.mtime);
                fi.change_time = RemoteFs::nt_time_from_system_time(attr.ctime);
                fi.index_number = ino as u64;

                println!(
                    "[OPEN] .10.1 Dir OpenFileInfo: cr={:#x} wt={:#x}",
                    fi.creation_time, fi.last_write_time
                );

                return Ok(MyFileContext {
                    ino,
                    is_dir: true,
                    allow_delete: wants_delete,
                    delete_on_close: AtomicBool::new(false),
                    temp_write: None,
                    needs_truncate: AtomicBool::new(false),
                    access_mask: granted_access,
                });
            } else {
                // Fallback in caso di cache miss per una directory
                eprintln!(
                    "[OPEN] .10.2 Directory '{}' not in cache after creation. Returning NOT FOUND to force re-evaluation.",
                    rel
                );
                return Err(FspError::WIN32(ERROR_FILE_NOT_FOUND));
            }
        }

        // 7) File: prova attr_cache; fallback a DirectoryEntry
        if let Some(mut attr) = self.get_attr_cache(&child_path) {
            println!("[OPEN] .11 attr cache HIT for '{}'", child_path.display());
            println!("[OPEN] .11 attr: size={} de.size={}", attr.size, de.size);
            // ⭐ NUOVO: Se size=0 ma il file esiste, verifica con backend
            if attr.size == 0 && de.size > 0 {
                println!(
                    "[OPEN] .11.1 Backend reports size={}, updating cache",
                    de.size
                );
                attr.size = de.size as u64;
                attr.blocks = (attr.size + 511) / 512;
                attr.mtime = std::time::UNIX_EPOCH
                    .checked_add(std::time::Duration::from_secs(de.mtime as u64))
                    .unwrap_or_else(std::time::SystemTime::now);

                self.insert_attr_cache(child_path.clone(), attr.clone());
            }

            let readonly = (attr.perm & 0o222) == 0;

            // ✅ USA attr AGGIORNATO
            fi.file_attributes = if readonly {
                FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_READONLY
            } else {
                FILE_ATTRIBUTE_NORMAL
            };
            fi.file_size = attr.size; // ✅ Ora è corretto!
            fi.allocation_size = ((attr.size + 4095) / 4096) * 4096;
            fi.creation_time = RemoteFs::nt_time_from_system_time(attr.crtime);
            fi.last_access_time = RemoteFs::nt_time_from_system_time(attr.atime);
            fi.last_write_time = RemoteFs::nt_time_from_system_time(attr.mtime);
            fi.change_time = RemoteFs::nt_time_from_system_time(attr.ctime);
            fi.index_number = ino as u64;
            fi.hard_links = 0;
            fi.reparse_tag = 0;
            fi.ea_size = 0;

            // ⭐ CRITICAL DEBUG: stampa TUTTO
            println!(
                "[OPEN] .11 OpenFileInfo: attrs={:#x} size={} alloc={} cr={:#x} at={:#x} wt={:#x} ct={:#x} idx={} hl={} rt={} ea={}",
                fi.file_attributes,
                fi.file_size,
                fi.allocation_size,
                fi.creation_time,
                fi.last_access_time,
                fi.last_write_time,
                fi.change_time,
                fi.index_number,
                fi.hard_links,
                fi.reparse_tag,
                fi.ea_size
            );
        } else {
            println!("[OPEN] .12 attr cache MISS - using backend DirectoryEntry values");

            fi.file_attributes = FILE_ATTRIBUTE_NORMAL;
            fi.file_size = de.size.max(0) as u64;
            fi.allocation_size = ((de.size.max(0) as u64 + 4095) / 4096) * 4096;

            let t = std::time::UNIX_EPOCH
                .checked_add(std::time::Duration::from_secs(de.mtime as u64))
                .unwrap_or_else(std::time::SystemTime::now);
            let nt = RemoteFs::nt_time_from_system_time(t);

            fi.creation_time = nt;
            fi.last_access_time = nt;
            fi.last_write_time = nt;
            fi.change_time = nt;
            fi.index_number = ino as u64;
            fi.hard_links = 0;
            fi.reparse_tag = 0;
            fi.ea_size = 0;

            // ⭐ CRITICAL DEBUG
            println!(
                "[OPEN] .12 OpenFileInfo: attrs={:#x} size={} alloc={} cr={:#x} at={:#x} wt={:#x} ct={:#x}",
                fi.file_attributes,
                fi.file_size,
                fi.allocation_size,
                fi.creation_time,
                fi.last_access_time,
                fi.last_write_time,
                fi.change_time
            );
        }
        // 8) TempWrite solo se wants_write
        let temp_write = if wants_write {
            println!(
                "[OPEN] .13 wants_write=true -> create temp file for ino={}",
                ino
            );
            let temp_path = self.get_temporary_path(ino);

            let should_prepopulate = wants_read;
            let is_truncate = wants_write && !wants_read;

            if is_truncate {
                println!("[OPEN] .13.1 TRUNCATE mode -> creating EMPTY temp file NOW");
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&temp_path)
                    .map_err(|e| {
                        eprintln!("[OPEN] ERROR creating empty temp: {}", e);
                        FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
                    })?;
                println!(
                    "[OPEN] .13.2 Empty temp file created at '{}'",
                    temp_path.display()
                );

                // ⭐ NUOVO: Log del contenuto iniziale del temp
                if let Ok(metadata) = std::fs::metadata(&temp_path) {
                    println!("[OPEN] .13.2.1 Temp file initial size: {}", metadata.len());
                }
            } else if should_prepopulate {
                // Caso: append o read+write
                println!("[OPEN] .13.3 wants_read=true -> pre-populating temp");

                // Ensure we have attributes (size) available for this entry
                let mut attr = self.get_attr_cache(&child_path);
                if attr.is_none() {
                    let is_dir = Self::is_dir(&de);
                    let ty = if is_dir { NodeType::Directory } else { NodeType::RegularFile };
                    let perm = Self::parse_perm(&de.permissions);
                    let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                    let a = self.file_attr(&child_path, ty, size, Some(de.mtime), perm);
                    self.insert_attr_cache(child_path.clone(), a.clone());
                    attr = Some(a);
                }

                let attr = match attr {
                    Some(a) => a,
                    None => {
                        println!("[OPEN] .13.3 No attr available -> create empty temp");
                        std::fs::File::create(&temp_path).map_err(|e| {
                            FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
                        })?;
                        // Build a minimal FileAttr for the newly-created empty temp
                        let is_dir = Self::is_dir(&de);
                        let ty = if is_dir { NodeType::Directory } else { NodeType::RegularFile };
                        let perm = Self::parse_perm(&de.permissions);
                        let a = self.file_attr(&child_path, ty, 0, Some(de.mtime), perm);
                        self.insert_attr_cache(child_path.clone(), a.clone());
                        a
                    }
                };

                if attr.size == 0 {
                    println!("[OPEN] .13.4 attr.size==0 -> create empty temp");
                    std::fs::File::create(&temp_path).map_err(|e| {
                        FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
                    })?;
                } else {
                    let start_u64 = 0u64;
                    let end_u64 = attr.size.saturating_sub(1);
                    match self.rt.block_on(self.api.read_range(&rel, start_u64, end_u64)) {
                        Ok(existing_data) if !existing_data.is_empty() => {
                            if let Err(e) = std::fs::write(&temp_path, &existing_data) {
                                eprintln!("[OPEN] WARN: pre-populate failed: {}", e);
                                std::fs::File::create(&temp_path).map_err(|e| {
                                    FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
                                })?;
                            } else {
                                println!(
                                    "[OPEN] .13.4 Pre-populated temp with {} bytes",
                                    existing_data.len()
                                );
                            }
                        }
                        Ok(_) => {
                            // Backend returned an empty body -> create empty temp
                            println!("[OPEN] .13.5 Backend returned empty -> create empty temp");
                            std::fs::File::create(&temp_path).map_err(|e| {
                                FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
                            })?;
                        }
                        Err(e) => {
                            eprintln!("[OPEN] .13.5 Backend read failed: {} -> trying fallback", e);
                            // fallback rel form
                            let alt = if rel.starts_with("./") {
                                rel.trim_start_matches("./").to_string()
                            } else {
                                format!("./{}", rel.trim_start_matches("./"))
                            };
                            match self.rt.block_on(self.api.read_range(&alt, start_u64, end_u64)) {
                                Ok(d2) if !d2.is_empty() => {
                                    if let Err(e) = std::fs::write(&temp_path, &d2) {
                                        eprintln!("[OPEN] WARN: fallback pre-populate failed: {}", e);
                                        std::fs::File::create(&temp_path).map_err(|e| {
                                            FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
                                        })?;
                                    } else {
                                        println!("[OPEN] .13.4 Fallback pre-populated temp with {} bytes", d2.len());
                                    }
                                }
                                Ok(_) => {
                                    println!("[OPEN] .13.6 Fallback returned empty -> create empty temp");
                                    std::fs::File::create(&temp_path).map_err(|e| {
                                        FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
                                    })?;
                                }
                                Err(e2) => {
                                    eprintln!("[OPEN] fallback read also failed: {} -> create empty temp", e2);
                                    std::fs::File::create(&temp_path).map_err(|e| {
                                        FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
                                    })?;
                                }
                            }
                        }
                    }
                }
            } else {
                // Fallback
                println!("[OPEN] .13.6 Fallback: create empty temp");
                std::fs::File::create(&temp_path).map_err(|e| {
                    FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
                })?;
            }

            // Verifica che il file esista
            if !temp_path.exists() {
                eprintln!(
                    "[OPEN] CRITICAL ERROR: temp file not created at '{}'",
                    temp_path.display()
                );
                return Err(FspError::WIN32(ERROR_INVALID_PARAMETER));
            }

            let size = std::fs::metadata(&temp_path).map(|m| m.len()).unwrap_or(0);

            println!(
                "[OPEN] .14 Temp file verified: exists={} size={}",
                temp_path.exists(),
                size
            );

            let tw = TempWrite {
                tem_path: temp_path,
                size,
            };
            self.state.writes.lock().unwrap().insert(ino, tw.clone());
            println!("[OPEN] .15 temp_write inserted for ino={}", ino);
            Some(tw)
        } else {
            println!("[OPEN] .13 wants_write=false -> no temp file");
            None
        };

        println!("[OPEN] .16 done for file '{}'", rel);

        Ok(MyFileContext {
            ino,
            is_dir: false,
            allow_delete: wants_delete,
            delete_on_close: AtomicBool::new(false),
            temp_write,
            needs_truncate: AtomicBool::new(false), // Non serve più il flag lazy
            access_mask: granted_access,
        })
    }

    fn close(&self, file_context: Self::FileContext) {
        println!(
            "[CLOSE] ⭐⭐⭐ ENTRY ino={} temp_write={}",
            file_context.ino,
            file_context.temp_write.is_some()
        );

        let temp_write = match file_context.temp_write {
            Some(tw) => tw,
            None => {
                println!("[CLOSE] no temp_write -> nothing to sync");
                return;
            }
        };

        if !temp_write.tem_path.exists() {
            eprintln!(
                "[CLOSE] ERROR: temp file missing at '{}' - skipping sync",
                temp_write.tem_path.display()
            );
            return;
        }

        let real_size = match std::fs::metadata(&temp_write.tem_path) {
            Ok(m) => {
                println!("[CLOSE] temp file metadata OK: size={}", m.len());
                m.len()
            }
            Err(e) => {
                eprintln!(
                    "[CLOSE] Failed to get temp file metadata for '{}': {}",
                    temp_write.tem_path.display(),
                    e
                );
                return;
            }
        };

        // ⭐ ANALISI FINALE
        if real_size == 0 {
            println!("[CLOSE] ⚠️⚠️⚠️ CRITICAL: Syncing EMPTY file!");
            println!("[CLOSE] This means write() was NEVER called");
            println!("[CLOSE] PowerShell might be using a different API");
        }

        let rel_path = RemoteFs::rel_of(&self.path_of(file_context.ino).unwrap());

        println!(
            "[CLOSE] syncing rel='{}' from temp='{}' (real_size={})",
            rel_path,
            temp_write.tem_path.display(),
            real_size
        );

        // 1) Commit sul backend
        if let Err(e) = self.rt.block_on(
            self.api
                .write_file(&rel_path, &temp_write.tem_path.to_string_lossy()),
        ) {
            eprintln!("[CLOSE] Errore commit file {}: {:?}", rel_path, e);
        } else {
            // 2) Aggiorna cache dopo commit riuscito
            let parent_rel = Path::new(&rel_path)
                .parent()
                .map(|p| p.to_string_lossy().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| ".".to_string());
            let parent_key = PathBuf::from(parent_rel.clone());

            println!(
                "[CLOSE] refreshing parent '{}' to update attr cache",
                parent_rel
            );

            if let Ok(list) = self.rt.block_on(self.api.ls(&parent_rel)) {
                if let Some(de) = list.into_iter().find(|d| {
                    d.name
                        == Path::new(&rel_path)
                            .file_name()
                            .and_then(|n| Some(n.to_string_lossy().to_string()))
                            .unwrap_or_default()
                }) {
                    let child = if parent_rel == "." {
                        PathBuf::from(format!("./{}", de.name))
                    } else {
                        PathBuf::from(format!(
                            "./{}/{}",
                            parent_rel.trim_start_matches("./"),
                            de.name
                        ))
                    };
                    let size = if RemoteFs::is_dir(&de) {
                        0
                    } else {
                        de.size.max(0) as u64
                    };
                    let backend_perm = RemoteFs::parse_perm(&de.permissions) as u16;
                    // Prefer cached perm if available (e.g. create/set_basic_info set desired perm)
                    let ty = if RemoteFs::is_dir(&de) {
                        NodeType::Directory
                    } else {
                        NodeType::RegularFile
                    };
                    // Check cache for an intended perm override (created earlier)
                    let cached_perm_opt = self.get_attr_cache(&child).map(|a| a.perm);
                    let final_perm: u16 = if let Some(cperm) = cached_perm_opt {
                        if cperm != backend_perm {
                            println!(
                                "[CLOSE] backend perm {:#o} != cached perm {:#o} -> reapplying cached perm",
                                backend_perm, cperm
                            );
                            // Try to reapply cached perm to backend
                            let _ = self
                                .rt
                                .block_on(self.api.chmod(&rel_path, cperm as u32))
                                .map_err(|e| eprintln!("[CLOSE] chmod post-commit failed: {}", e));
                            cperm
                        } else {
                            backend_perm
                        }
                    } else {
                        backend_perm
                    };
                    let perm = final_perm as u16;
                    let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm);
                    println!(
                        "[CLOSE] updating attr_cache for '{}' size={}",
                        child.display(),
                        size
                    );
                    self.insert_attr_cache(child, attr);
                }
                let _ = self.update_cache(&parent_key);
            }
        }
        /*
                // 3) chmod post-commit
                // Preserve readonly if backend/cache says file was read-only (0o444).
                let desired_mode = if let Some(attr) = self.get_attr_cache(&PathBuf::from(&rel_path)) {
                    println!(
                        "[CLOSE] setting permissions based on attr cache perm={:#o}",
                        attr.perm
                    );
                    if (attr.perm & 0o444) == 1 {
                        0o444
                    } else {
                        0o644
                    }
                } else {
                    // fallback: ask backend parent listing for the entry's permissions
                    let parent_rel = Path::new(&rel_path)
                        .parent()
                        .map(|p| p.to_string_lossy().to_string())
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| ".".to_string());
                    match self.rt.block_on(self.api.ls(&parent_rel)) {
                        Ok(list) => {
                            let name = Path::new(&rel_path)
                                .file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or_default();
                            if let Some(de) = list.into_iter().find(|d| d.name == name) {
                                let perm = RemoteFs::parse_perm(&de.permissions);
                                if (perm & 0o222) == 0 { 0o444 } else { 0o644 }
                            } else {
                                0o644
                            }
                        }
                        Err(_) => 0o644,
                    }
                };
                println!(
                    "[CHMOD] applying chmod {} to '{}'",
                    desired_mode, rel_path
                );
                let _ = self.rt.block_on(self.api.chmod(&rel_path, desired_mode));
        */
        // 4) Pulisci il temp file
        if let Err(e) = std::fs::remove_file(&temp_write.tem_path) {
            eprintln!("[CLOSE] Errore rimozione temp file: {}", e);
        }
        self.state.writes.lock().unwrap().remove(&file_context.ino);
        println!("[CLOSE] done for '{}'", rel_path);
    }

    fn read(
        &self,
        file_context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> WinFspResult<u32> {
        println!(
            "[READ] entry ino={} offset={} temp_write={}",
            file_context.ino,
            offset,
            file_context.temp_write.is_some()
        );
        let path = self.path_of(file_context.ino).ok_or(FspError::WIN32(1))?;
        let rel_path = RemoteFs::rel_of(&path);
        println!("[READ] rel='{}'", rel_path);

        // Ensure we have attributes (size) for this path; if missing, try to populate from parent listing
        let mut attr = self.get_attr_cache(&path);

        if attr.is_none() {
            let parent_rel = Path::new(&rel_path)
                .parent()
                .map(|p| p.to_string_lossy().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| ".".to_string());
            let parent_key = PathBuf::from(parent_rel.clone());

            match self.dir_entries(&parent_key) {
                Ok(entries) => {
                    if let Some((child_path, de)) = entries.into_iter().find(|(p, _)| *p == path)
                    {
                        let is_dir = Self::is_dir(&de);
                        let ty = if is_dir {
                            NodeType::Directory
                        } else {
                            NodeType::RegularFile
                        };
                        let perm = Self::parse_perm(&de.permissions);
                        let size = if is_dir { 0 } else { de.size.max(0) as u64 };

                        let a = self.file_attr(&path, ty, size, Some(de.mtime), perm);
                        self.insert_attr_cache(path.clone(), a.clone());
                        attr = Some(a);
                    }
                }
                Err(_) => {
                    return Err(FspError::WIN32(ERROR_FILE_NOT_FOUND));
                }
            }
        }

        let attr = match attr {
            Some(a) => a,
            None => return Err(FspError::WIN32(ERROR_FILE_NOT_FOUND)),
        };

        // If offset beyond EOF, return 0 bytes
        if offset as u64 >= attr.size {
            return Ok(0);
        }

        if buffer.is_empty() {
            return Ok(0);
        }

        // Compute start/end for backend range reads
        let start_u64 = offset as u64;
        let end_u64 = (start_u64 + (buffer.len() as u64) - 1).min(attr.size.saturating_sub(1));

        let data: Vec<u8> = if let Some(tw) = &file_context.temp_write {
            println!("[READ] reading from temp '{}'", tw.tem_path.display());
            match std::fs::read(&tw.tem_path) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("[READ] failed read temp: {}", e);
                    return Err(FspError::from(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )));
                }
            }
        } else {
            println!("[READ] reading from backend with rel='{}'", rel_path);
            match self.rt.block_on(self.api.read_range(&rel_path, start_u64, end_u64)) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("[READ] backend read failed for '{}': {}", rel_path, e);
                    // fallback (if backend expects './' form)
                    let alt = if rel_path.starts_with("./") {
                        rel_path.trim_start_matches("./").to_string()
                    } else {
                        format!("./{}", rel_path.trim_start_matches("./"))
                    };
                    eprintln!("[READ] trying fallback rel='{}'", alt);
                    match self.rt.block_on(self.api.read_range(&alt, start_u64, end_u64)) {
                        Ok(d2) => d2,
                        Err(e2) => {
                            eprintln!("[READ] backend read fallback failed for '{}': {}", alt, e2);
                            return Err(FspError::from(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e2.to_string(),
                            )));
                        }
                    }
                }
            }
        };

        let bytes_to_copy: &[u8];
        if file_context.temp_write.is_some() {
            let start = offset as usize;
            if start >= data.len() {
                println!("[READ] offset >= data.len -> return 0");
                return Ok(0);
            }
            let end = std::cmp::min(start + buffer.len(), data.len());
            bytes_to_copy = &data[start..end];
        } else {
            // backend returned exactly the requested window starting at `start_u64`
            let end = std::cmp::min(buffer.len(), data.len());
            bytes_to_copy = &data[0..end];
        }
        buffer[..bytes_to_copy.len()].copy_from_slice(bytes_to_copy);
        println!(
            "[READ] copied {} bytes ({} of {})",
            bytes_to_copy.len(),
            offset,
            data.len()
        );
        Ok(bytes_to_copy.len() as u32)
    }

    fn write(
        &self,
        file_context: &Self::FileContext,
        buffer: &[u8],
        offset: u64,
        write_to_end_of_file: bool,
        constrained_io: bool,
        file_info: &mut FileInfo,
    ) -> WinFspResult<u32> {
        println!(
            "[WRITE] ⭐⭐⭐ CALLED! ino={} offset={} len={} write_to_eof={} constrained={}",
            file_context.ino,
            offset,
            buffer.len(),
            write_to_end_of_file,
            constrained_io
        );

        // Check permissions: se il file è readonly (nessun bit di scrittura), rifiuta
        if let Some(path) = self.path_of(file_context.ino) {
            if let Some(attr) = self.get_attr_cache(&path) {
                if (attr.perm & 0o222) == 0 {
                    println!(
                        "[WRITE] denied by perm for {:?} perm={:#o}",
                        path, attr.perm
                    );
                    return Err(FspError::WIN32(ERROR_ACCESS_DENIED));
                }
            }
        }

        // ⭐ DUMP dei primi bytes per debug
        if buffer.len() > 0 {
            let preview =
                std::str::from_utf8(&buffer[..buffer.len().min(50)]).unwrap_or("<binary>");
            println!("[WRITE] buffer preview: {:?}", preview);
        }
        let tw = match &file_context.temp_write {
            Some(tw) => tw,
            None => return Err(FspError::WIN32(1)), // file opened read-only
        };

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&tw.tem_path)
            .map_err(|e| {
                eprintln!("[WRITE] ERROR opening temp: {}", e);
                FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
            })?;

        println!("[WRITE] Seeking to offset {}", offset);

        file.seek(std::io::SeekFrom::Start(offset)).map_err(|e| {
            eprintln!("[WRITE] ERROR seeking: {}", e);
            FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
        })?;

        println!("[WRITE] Writing {} bytes", buffer.len());

        file.write_all(buffer).map_err(|e| {
            eprintln!("[WRITE] ERROR writing: {}", e);
            FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
        })?;

        file.flush().map_err(|e| {
            eprintln!("[WRITE] ERROR flushing: {}", e);
            FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
        })?;

        if let Ok(metadata) = std::fs::metadata(&tw.tem_path) {
            let new_size = metadata.len();
            file_info.file_size = new_size;
            file_info.allocation_size = ((new_size + 4095) / 4096) * 4096;
            println!(
                "[WRITE] ⭐ Success: wrote {} bytes, total size now {}",
                buffer.len(),
                new_size
            );
        }

        Ok(buffer.len() as u32)
    }

    fn overwrite(
        &self,
        context: &Self::FileContext,
        file_attributes: FILE_FLAGS_AND_ATTRIBUTES,
        replace_file_attributes: bool,
        allocation_size: u64,
        extra_buffer: Option<&[u8]>,
        file_info: &mut FileInfo,
    ) -> Result<(), FspError> {
        println!(
            "[OVERWRITE] ino={} replace_attrs={} allocation_size={}",
            context.ino, replace_file_attributes, allocation_size
        );

        if let Some(tw) = &context.temp_write {
            println!(
                "[OVERWRITE] truncating temp file '{}'",
                tw.tem_path.display()
            );
            let result = std::fs::OpenOptions::new()
                .write(true)
                .open(&tw.tem_path)
                .and_then(|f| f.set_len(0));
            if let Err(e) = result {
                eprintln!("[OVERWRITE] ERROR truncating temp file: {}", e);
                return Err(FspError::from(io::Error::new(
                    io::ErrorKind::Other,
                    e.to_string(),
                )));
            }
            println!("[OVERWRITE] temp file truncated successfully");
        } else {
            eprintln!("[OVERWRITE] No temp_write available for truncation");
            return Err(FspError::WIN32(1));
        }

        Ok(())
    }

    fn read_directory(
        &self,
        file_context: &Self::FileContext,
        _pattern: Option<&widestring::U16CStr>,
        marker: DirMarker<'_>,
        buffer: &mut [u8],
    ) -> WinFspResult<u32> {
        println!("Siamo in read_dir");
        let dir_path = self.path_of(file_context.ino).ok_or(FspError::WIN32(1))?;

        let mut entries = self.dir_entries(&dir_path)?;
        let marker_name: Option<String> = marker
            .inner_as_cstr()
            .map(|w: &U16CStr| w.to_string_lossy().to_string());

        entries.sort_by(|a, b| a.1.name.cmp(&b.1.name));
        let iter = entries.into_iter().filter(|(_, de)| {
            if let Some(ref m) = marker_name {
                de.name > *m
            } else {
                true
            }
        });

        let mut bytes_transferred: u32 = 0;

        for (_, de) in iter {
            let name_w = match U16CString::from_str(&de.name) {
                Ok(n) => n,
                Err(_) => continue,
            };
            let name_slice = name_w.as_slice();
            let name_len = name_slice.len();

            let mut entry_size = core::mem::size_of::<FSP_FSCTL_DIR_INFO>() + name_len * 2;
            entry_size = (entry_size + 7) & !7;
            let entry_size = entry_size as u16;

            #[repr(align(8))]
            struct AlignedBuffer([u8; 4096]);

            let mut raw = AlignedBuffer([0u8; 4096]);

            if (entry_size as usize) > raw.0.len() {
                break;
            }

            let dir_info_ptr = raw.0.as_mut_ptr() as *mut FSP_FSCTL_DIR_INFO;

            unsafe {
                core::ptr::write_bytes(dir_info_ptr as *mut u8, 0, entry_size as usize);

                (*dir_info_ptr).Size = entry_size;

                // Determina se è una directory o un file
                let is_dir = Self::is_dir(&de);

                // Imposta gli attributi
                (*dir_info_ptr).FileInfo.FileAttributes = if is_dir {
                    FILE_ATTRIBUTE_DIRECTORY
                } else {
                    FILE_ATTRIBUTE_NORMAL
                };

                // DISTINZIONE: Imposta dimensioni SOLO per i file, NON per le directory
                if is_dir {
                    // Per le directory: FileSize e AllocationSize devono essere 0
                    (*dir_info_ptr).FileInfo.FileSize = 0;
                    (*dir_info_ptr).FileInfo.AllocationSize = 0;
                } else {
                    // Per i file: usa la dimensione effettiva dal backend
                    let file_size = de.size as u64;
                    (*dir_info_ptr).FileInfo.FileSize = file_size;

                    // Calcola AllocationSize arrotondato al cluster (4096 byte)
                    let cluster = 4096u64;
                    let alloc = if file_size == 0 {
                        0
                    } else {
                        ((file_size + cluster - 1) / cluster) * cluster
                    };
                    (*dir_info_ptr).FileInfo.AllocationSize = alloc;
                }

                // Timestamp (uguali per file e directory)
                let mtime = UNIX_EPOCH
                    .checked_add(Duration::from_secs(de.mtime as u64))
                    .unwrap_or_else(SystemTime::now);
                let t = RemoteFs::nt_time_from_system_time(mtime);
                (*dir_info_ptr).FileInfo.CreationTime = t;
                (*dir_info_ptr).FileInfo.LastAccessTime = t;
                (*dir_info_ptr).FileInfo.LastWriteTime = t;
                (*dir_info_ptr).FileInfo.ChangeTime = t;

                // Copia del nome subito dopo la struttura
                let name_dst = (dir_info_ptr as *mut u8)
                    .add(core::mem::size_of::<FSP_FSCTL_DIR_INFO>())
                    as *mut u16;
                core::ptr::copy_nonoverlapping(name_slice.as_ptr(), name_dst, name_len);

                // Aggiungi l'entry al buffer di risposta
                let ok = FspFileSystemAddDirInfo(
                    dir_info_ptr,
                    buffer.as_mut_ptr() as *mut _,
                    buffer.len() as u32,
                    core::ptr::addr_of_mut!(bytes_transferred),
                );

                if ok == 0 {
                    break;
                }
            }
        }

        // Segnala EOF
        unsafe {
            let _ = FspFileSystemAddDirInfo(
                core::ptr::null_mut(),
                buffer.as_mut_ptr() as *mut _,
                buffer.len() as u32,
                core::ptr::addr_of_mut!(bytes_transferred),
            );
        }

        Ok(bytes_transferred)
    }

    fn create(
        &self,
        path: &U16CStr,
        create_options: u32,
        granted_access: u32,
        _file_attributes: u32,
        _allocation_size: Option<&[c_void]>,
        _create_flags: u64,
        _reserved: Option<&[u8]>,
        _write_through: bool,
        file_info: &mut OpenFileInfo,
    ) -> WinFspResult<Self::FileContext> {
        println!("Siamo in create");

        let path_str = self.path_from_u16(path);
        let rel = RemoteFs::rel_of(Path::new(&path_str));
        let is_dir = (create_options & CREATE_DIRECTORY) != 0;

        let now = SystemTime::now();
        let nt_time = RemoteFs::nt_time_from_system_time(now);
        let fi = file_info.as_mut();

        let parent_rel = Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());

        println!("[CREATE] caso dir parentpath : {}", parent_rel);

        let parent_path = PathBuf::from(&parent_rel);

        //Caso Directory
        if is_dir {
            match self.rt.block_on(self.api.mkdir(&rel)) {
                Ok(_) => {
                    fi.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
                    fi.file_size = 0;
                    fi.creation_time = nt_time;
                    fi.last_access_time = nt_time;
                    fi.last_write_time = nt_time;
                    fi.change_time = nt_time;

                    let ino = self.alloc_ino(std::path::Path::new(&path_str));

                    // 1) Aggiorna cache parent: ricarica elenco (Explorer leggerà subito)
                    let _ = self.update_cache(&parent_path);

                    // 2) Inserisci attr della nuova dir in cache (evita finestra di inconsistenza)
                    let mut attr = self.file_attr(
                        std::path::Path::new(&path_str),
                        NodeType::Directory,
                        0,
                        None,
                        0o755,
                    );
                    attr.nlink = 2; // directory tipicamente 2
                    self.insert_attr_cache(std::path::PathBuf::from(&rel), attr);

                    return Ok(MyFileContext {
                        ino,
                        temp_write: None,
                        delete_on_close: std::sync::atomic::AtomicBool::new(false),
                        is_dir: true,
                        allow_delete: (granted_access & DELETE) != 0,
                        needs_truncate: AtomicBool::new(false),
                        access_mask: 0,
                    });
                }
                Err(e) => {
                    eprintln!("[CREATE] mkdir failed for '{}' -> {}", rel, e);
                    return Err(FspError::from(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )));
                }
            }
        }

        //Caso File

        // Esiste già --> errore
        if self.backend_entry_exists(&rel) {
            return Err(FspError::WIN32(ERROR_ALREADY_EXISTS));
        }

        let ino = self.alloc_ino(Path::new(&path_str));
        println!("[CREATE] file ino: {:?}", ino);

        // 1. Crea il file temporaneo vuoto PRIMA di chiamare write_file
        let temp_path = self.get_temporary_path(ino);
        if let Err(e) = std::fs::File::create(&temp_path) {
            eprintln!("[CREATE] Errore creazione file temporaneo: {}", e);
            return Err(FspError::WIN32(ERROR_INVALID_PARAMETER as u32));
        }

        //aggiungo la creazione immediata del file vuoto per la gui (di explorer) che mi permette di fare la creazione file
        match self
            .rt
            .block_on(self.api.write_file(&rel, &temp_path.to_str().unwrap()))
        {
            Ok(_) => {
                // 2. Imposta mode desiderato per il nuovo file (evita ereditare 755 dalla dir)
                /*  let desired_mode: u32 = 0o644;
                let _ = self
                    .rt
                    .block_on(self.api.chmod(&rel, desired_mode))
                    .map_err(|e| {
                        eprintln!("[CREATE] warning: chmod after create failed: {}", e);
                        FspError::from(io::Error::new(io::ErrorKind::Other, e.to_string()))
                    });*/
                let desired_mode: u32 = 0o644;
                // 3. Prepara la struttura per le scritture temporanee (locale)
                let temp_path = self.get_temporary_path(ino);
                if let Err(e) = std::fs::File::create(&temp_path) {
                    eprintln!("[CREATE] Errore creazione file temporaneo: {}", e);
                    return Err(FspError::WIN32(ERROR_INVALID_PARAMETER as u32));
                }
                let temp_write = TempWrite {
                    tem_path: temp_path,
                    size: 0,
                };
                self.state
                    .writes
                    .lock()
                    .unwrap()
                    .insert(ino, temp_write.clone());

                // 4. Costruisci FileContext
                let file_context = MyFileContext {
                    ino,
                    temp_write: Some(temp_write),
                    delete_on_close: AtomicBool::new(false),
                    allow_delete: (granted_access & DELETE) != 0,
                    is_dir: false,
                    needs_truncate: AtomicBool::new(false),
                    access_mask: 0,
                };
                // 5. Popola OpenFileInfo e cache coerente (perm = desired_mode)
                fi.file_attributes = FILE_ATTRIBUTE_NORMAL;
                fi.file_size = 0;
                fi.creation_time = nt_time;
                fi.last_access_time = nt_time;
                fi.last_write_time = nt_time;
                fi.change_time = nt_time;

                let mut attr = self.file_attr(
                    Path::new(&path_str),
                    NodeType::RegularFile,
                    0,
                    None,
                    desired_mode as u16,
                );
                attr.nlink = 1;
                self.insert_attr_cache(Path::new(&rel).to_path_buf(), attr);

                // 6. Aggiorna cache parent per rendere visibile la nuova entry
                let _ = self.update_cache(&parent_path);

                Ok(file_context)
            }
            Err(e) => {
                eprintln!("[CREATE] Errore creazione file sul backend: {}", e);
                //pulisci il file temporaneo locale se write fallisce
                let _ = std::fs::remove_file(&temp_path);
                Err(FspError::WIN32(ERROR_INVALID_PARAMETER as u32))
            }
        }
    }

    //per la modifica dei permessi
    fn set_basic_info(
        &self,
        file_context: &Self::FileContext,
        file_attributes: u32,
        creation_time: u64,
        last_access_time: u64,
        last_write_time: u64,
        change_time: u64,
        file_info: &mut FileInfo,
    ) -> WinFspResult<()> {
        let path = self.path_of(file_context.ino).ok_or(FspError::WIN32(
            windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
        ))?;

        let rel = RemoteFs::rel_of(&path);
        let rel_key = PathBuf::from(rel.clone());
        let parent_rel = std::path::Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let parent_key = PathBuf::from(parent_rel.clone());

        println!(
            "[SET_BASIC_INFO] ino={} path='{}' file_attrs={:#x} access_mask={:#x}",
            file_context.ino, rel, file_attributes, file_context.access_mask
        );

        // 1) Get or create attr from cache
        let mut attr = if let Some(a) = self.get_attr_cache(&rel_key) {
            a
        } else {
            match self.dir_entries(&parent_key) {
                Ok(entries) => {
                    if let Some((p, de)) = entries.into_iter().find(|(p, _)| *p == rel_key) {
                        let is_dir = Self::is_dir(&de);
                        let ty = if is_dir {
                            NodeType::Directory
                        } else {
                            NodeType::RegularFile
                        };
                        let perm = Self::parse_perm(&de.permissions);
                        let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                        let a = self.file_attr(&p, ty, size, Some(de.mtime), perm);
                        self.insert_attr_cache(p.clone(), a.clone());
                        a
                    } else {
                        return Err(FspError::WIN32(
                            windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
                        ));
                    }
                }
                Err(_) => {
                    return Err(FspError::WIN32(
                        windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
                    ));
                }
            }
        };

        // 2) Determine if we should apply permission changes
        // Critical logic: ONLY use temp_write to detect active write operations
        // DO NOT check access_mask - it has too many false positives due to bit overlaps

        let has_pending_write = file_context.temp_write.is_some();

        println!(
            "[SET_BASIC_INFO] Write detection: has_pending_write={} access_mask={:#x} file_attrs={:#x}",
            has_pending_write, file_context.access_mask, file_attributes
        );

        let mode = if has_pending_write {
            // CASE A: File has pending writes (copy/paste, create with immediate write)
            // Ignore FILE_ATTRIBUTE_READONLY - Explorer sets this during copy
            println!(
                "[SET_BASIC_INFO] File has pending write - ignoring FILE_ATTRIBUTE_READONLY flag"
            );

            // Keep existing permission or default to writable
            if attr.perm == 0 {
                0o644 // Default for new files
            } else {
                attr.perm as u32 // Preserve existing permission
            }
        } else if file_attributes != u32::MAX {
            // CASE B: Explicit attribute change without pending write
            // This is the Properties dialog case
            if (file_attributes & FILE_ATTRIBUTE_READONLY) != 0 {
                println!("[SET_BASIC_INFO] ✓ User set readonly attribute (no pending write)");
                0o444
            } else {
                println!("[SET_BASIC_INFO] ✓ User cleared readonly attribute (no pending write)");
                0o644
            }
        } else {
            // CASE C: No attribute change requested
            println!(
                "[SET_BASIC_INFO] No attribute change, preserving perm={:#o}",
                attr.perm
            );
            attr.perm as u32
        };

        println!("[SET_BASIC_INFO] Final decision: mode={:#o}", mode);

        // 3) Apply chmod to backend
        self.rt.block_on(self.api.chmod(&rel, mode)).map_err(|e| {
            eprintln!("[SET_BASIC_INFO] chmod failed: {}", e);
            FspError::from(io::Error::new(io::ErrorKind::Other, format!("{}", e)))
        })?;

        // 4) Handle timestamps - utimes (Linux equivalent)
        let mut need_utimes = false;
        let mut new_atime = None;
        let mut new_mtime = None;

        if last_access_time != 0 {
            new_atime = RemoteFs::filetime_to_systemtime(last_access_time);
            if let Some(at) = new_atime {
                attr.atime = at;
                need_utimes = true;
            }
        }

        if last_write_time != 0 {
            new_mtime = RemoteFs::filetime_to_systemtime(last_write_time);
            if let Some(mt) = new_mtime {
                attr.mtime = mt;
                attr.ctime = mt;
                need_utimes = true;
            }
        }

        if need_utimes {
            self.rt
                .block_on(self.api.utimes(&rel, new_atime, new_mtime))
                .map_err(|e| {
                    eprintln!("[SET_BASIC_INFO] utimes failed: {}", e);
                    FspError::from(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;
        }

        // 5) Update cache with new permissions
        attr.perm = mode as u16;
        self.insert_attr_cache(rel_key.clone(), attr.clone());
        println!(
            "[SET_BASIC_INFO] ✓ Cache updated: perm={:#o} size={}",
            mode, attr.size
        );

        // 6) Refresh parent cache
        let _ = self.update_cache(&parent_key);

        // 7) Update WinFsp file_info
        if file_attributes != u32::MAX {
            file_info.file_attributes = file_attributes;
        }
        if creation_time != 0 {
            file_info.creation_time = creation_time;
        }
        if last_access_time != 0 {
            file_info.last_access_time = last_access_time;
        }
        if last_write_time != 0 {
            file_info.last_write_time = last_write_time;
        }
        if change_time != 0 {
            file_info.change_time = change_time;
        }

        Ok(())
    }

    fn get_dir_info_by_name(
        &self,
        context: &Self::FileContext,
        file_name: &U16CStr,
        out_dir_info: &mut DirInfo,
    ) -> WinFspResult<()> {
        println!(
            "[GET_DIR_INFO_BY_NAME] file_name={:?}",
            file_name.to_string_lossy()
        );

        let path = self.path_from_u16(file_name);
        let rel = RemoteFs::rel_of(std::path::Path::new(&path));

        println!("[GET_DIR_INFO_BY_NAME] rel='{}'", rel);

        // Root case
        if rel == "." {
            println!("[GET_DIR_INFO_BY_NAME] root directory");
            return Ok(());
        }

        // Determina parent e nome
        let parent_rel = Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());

        let name_only = Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        let parent_key = PathBuf::from(&parent_rel);

        // Usa dir_entries (cache-aware)
        let entries = self.dir_entries(&parent_key)?;

        // Cerca l'entry
        if let Some((_, _de)) = entries.iter().find(|(_, d)| d.name == name_only) {
            println!("[GET_DIR_INFO_BY_NAME] found entry: {}", _de.name);

            // 🌟 PASSO CRITICO: Recupera gli attributi dalla cache
            match self.get_attr_cache(Path::new(&rel)) {
                // ✅ CORRETTO: Gestisce il caso in cui l'attributo sia in cache (Some)
                Some(attr) => {
                    // Popola out_dir_info con i dati della cache
                    //out_dir_info.file_info_mut().file_attributes = attr.file_attributes;
                    out_dir_info.file_info_mut().file_size = attr.size;
                    out_dir_info.file_info_mut().creation_time =
                        RemoteFs::nt_time_from_system_time(attr.ctime);
                    out_dir_info.file_info_mut().last_write_time =
                        RemoteFs::nt_time_from_system_time(attr.ctime);
                    out_dir_info.file_info_mut().last_access_time =
                        RemoteFs::nt_time_from_system_time(attr.atime);
                    out_dir_info.file_info_mut().change_time =
                        RemoteFs::nt_time_from_system_time(attr.ctime);

                    Ok(())
                }
                // ✅ CORRETTO: Gestisce il caso in cui l'attributo NON sia in cache (None)
                None => {
                    eprintln!(
                        "[GET_DIR_INFO_BY_NAME] Attributi mancanti in cache per '{}'.",
                        rel
                    );

                    // Opzione 1: Prova a recuperare dal backend (consigliato per robustezza)
                    // Se hai una funzione self.api.get_attributes_by_path(&rel) che restituisce
                    // Result<FileAttr, E>, puoi chiamarla qui.

                    // Opzione 2: Ritorna FILE_NOT_FOUND (Se la cache è l'unica fonte attendibile)
                    // Questo potrebbe essere l'errore se la creazione è fallita, ma la dir_entries è corretta.
                    Err(FspError::WIN32(ERROR_FILE_NOT_FOUND))
                }
            }
        } else {
            println!("[GET_DIR_INFO_BY_NAME] entry not found: {}", name_only);
            Err(FspError::WIN32(ERROR_FILE_NOT_FOUND))
        }
    }

    //equivalente truncate per aumento dimensione di file in write
    fn set_file_size(
        &self,
        file_context: &Self::FileContext,
        new_size: u64,
        set_allocation_size: bool,
        file_info: &mut FileInfo,
    ) -> WinFspResult<()> {
        println!(
            "[SET_FILE_SIZE] ⭐ CALLED! ino={} new_size={} set_allocation={} has_temp={}",
            file_context.ino,
            new_size,
            set_allocation_size,
            file_context.temp_write.is_some()
        );

        let path = self.path_of(file_context.ino).ok_or(FspError::WIN32(
            windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
        ))?;
        let rel = RemoteFs::rel_of(&path);

        if let Some(tw) = &file_context.temp_write {
            println!(
                "[SET_FILE_SIZE] temp file path: '{}'",
                tw.tem_path.display()
            );

            // ⭐ CRITICO: Controlla se il file temp esiste e leggi il contenuto attuale
            if tw.tem_path.exists() {
                let current_size = std::fs::metadata(&tw.tem_path)
                    .map(|m| m.len())
                    .unwrap_or(0);

                println!(
                    "[SET_FILE_SIZE] temp file exists: current_size={} requested_size={}",
                    current_size, new_size
                );

                // Se la nuova size è MAGGIORE e il file è vuoto, PowerShell potrebbe star
                // cercando di allocare spazio per poi scrivere
                if new_size > current_size && current_size == 0 {
                    println!("[SET_FILE_SIZE] ⚠️ PowerShell allocating space without write()!");
                    println!("[SET_FILE_SIZE] This is the echo > file pattern");

                    // NON truncare - lascia il file vuoto
                    // PowerShell dovrebbe scrivere dopo, ma se non lo fa...
                    // potremmo dover intercettare in flush() o close()
                }

                // Esegui il truncate/extend normale
                if let Ok(mut f) = std::fs::OpenOptions::new().write(true).open(&tw.tem_path) {
                    if let Err(e) = f.set_len(new_size) {
                        eprintln!("[SET_FILE_SIZE] failed to set temp file size: {}", e);
                        return Err(FspError::from(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e.to_string(),
                        )));
                    }
                    println!("[SET_FILE_SIZE] temp file resized to {}", new_size);
                }

                // ⭐ VERIFICA POST-RESIZE
                if let Ok(metadata) = std::fs::metadata(&tw.tem_path) {
                    println!(
                        "[SET_FILE_SIZE] temp file after resize: size={}",
                        metadata.len()
                    );
                }
            } else {
                eprintln!("[SET_FILE_SIZE] ERROR: temp file doesn't exist!");
            }
        }

        // Backend truncate (potrebbe non essere necessario se il file è gestito solo localmente)
        self.rt
            .block_on(self.api.truncate(&rel, new_size))
            .map_err(|e| {
                eprintln!("[SET_FILE_SIZE] backend truncate failed: {}", e);
                FspError::from(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        // Aggiorna cache
        if let Some(mut attr) = self.get_attr_cache(&path) {
            attr.size = new_size;
            attr.blocks = (new_size + 511) / 512;
            attr.mtime = SystemTime::now();
            attr.ctime = attr.mtime;
            self.insert_attr_cache(path.clone(), attr);
        }

        file_info.file_size = new_size;
        file_info.allocation_size = ((new_size + 4095) / 4096) * 4096;

        println!(
            "[SET_FILE_SIZE] done: file_info.file_size={}",
            file_info.file_size
        );
        Ok(())
    }
    fn flush(
        &self,
        file_context: std::option::Option<&MyFileContext>,
        _file_info: &mut FileInfo,
    ) -> WinFspResult<()> {
        println!(
            "[FLUSH] ⭐ CALLED! ino={} has_temp={}",
            file_context.unwrap().ino,
            file_context.unwrap().temp_write.is_some()
        );
        // Se c'è un temp file, committalo subito
        if let Some(ref tw) = file_context.unwrap().temp_write {
            println!("[FLUSH] temp file: '{}'", tw.tem_path.display());

            if let Ok(metadata) = std::fs::metadata(&tw.tem_path) {
                println!("[FLUSH] temp file size: {}", metadata.len());

                // Se il file è vuoto, leggi da stdin o altra fonte?
                if metadata.len() == 0 {
                    println!("[FLUSH] ⚠️ WARNING: Flushing empty temp file!");
                }
            }

            let path = self
                .path_of(file_context.unwrap().ino)
                .ok_or(FspError::WIN32(
                    windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
                ))?;
            let rel = RemoteFs::rel_of(&path);

            let parent_rel = std::path::Path::new(&rel)
                .parent()
                .map(|p| p.to_string_lossy().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| ".".to_string());

            self.rt
                .block_on(self.api.write_file(&rel, &tw.tem_path.to_string_lossy()))
                .map_err(|e| {
                    let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                    FspError::from(io_err)
                })?;

            if let Ok(meta) = std::fs::metadata(&tw.tem_path) {
                // Se hai size locale, aggiorna l’entry
                if let Some(mut a) = self.get_attr_cache(&path) {
                    a.size = meta.len();
                    // Imposta un mtime conservativo “ora”; se hai un valore dal backend, meglio usarlo
                    let now = std::time::SystemTime::now();
                    a.mtime = now;
                    a.ctime = now;
                    self.insert_attr_cache(path.clone(), a);
                }
            }
            let parent_rel_str = parent_rel.as_str();
            let parent_path = Path::new(parent_rel_str);
            let _ = self.update_cache(parent_path);
        }

        Ok(())
    }

    fn get_reparse_point(
        &self,
        context: &Self::FileContext,
        file_name: &U16CStr,
        buffer: &mut [u8],
    ) -> Result<u64, FspError> {
        println!(
            "[GET_REPARSE_POINT] ino={} file_name={}",
            context.ino,
            file_name.to_string_lossy()
        );

        // Non supportiamo reparse point (symlink, junction, ecc.)
        // Ritorna 0 (nessun reparse point)
        Ok(0)
    }

    fn set_delete(
        &self,
        file_context: &MyFileContext,
        file_name: &U16CStr,
        delete: bool,
    ) -> WinFspResult<()> {
        println!(
            "set_delete: delete={} for path={:?}, ino={}",
            delete, file_name, file_context.ino
        );

        let percorso = Some(file_name);

        let path = if let Some(name) = percorso {
            let p = self.path_from_u16(name);
            println!("[CAN_DELETE] file_name provided -> path_from_u16 = {}", p);
            p
        } else {
            let p = self
                .path_of(file_context.ino)
                .map(|p| p.to_string_lossy().to_string())
                .ok_or(FspError::WIN32(
                    windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
                ))?;
            println!(
                "[CAN_DELETE] no file_name, path_of(ino={}) = {}",
                file_context.ino, p
            );
            p
        };

        let rel = RemoteFs::rel_of(std::path::Path::new(&path));

        //se non hai i permessi negi la cancellazione
        if let Some(attr) = self.get_attr_cache(&PathBuf::from(&rel)) {
            if (attr.perm & 0o222) == 0 {
                println!(
                    "[SET_DELETE] denied by perm for {:?} perm={:#o}",
                    path, attr.perm
                );
                return Err(FspError::WIN32(ERROR_ACCESS_DENIED));
            }
        }

        if delete {
            // Prima verifica se si può cancellare

            self.can_delete(file_context, rel)?;

            // 🔴 Importante: marca il contesto come "da cancellare al close"
            file_context.delete_on_close.store(true, Ordering::Relaxed);

            println!(
                "file_context {} marked delete_on_close = true",
                file_context.ino
            );
        } else {
            file_context.delete_on_close.store(false, Ordering::Relaxed);
        }

        Ok(()) //tornando ok dovrebbe marcare il fspCleanupDelete in modo da abilitare la cancellazione
    }

    //problema con la rimozione da windows powershell perchè rmdir non funziona per farlo funzionare bisogna chiamare cmd /c rmdir nome_cartella
    fn cleanup(&self, file_context: &MyFileContext, file_name: Option<&U16CStr>, flags: u32) {
        println!(
            "[CLEANUP] flags={:#x} FspCleanupDelete={:#x} delete_on_close={}",
            flags,
            FspCleanupDelete as u32,
            file_context.delete_on_close.load(Ordering::Relaxed)
        );

        //
        // 1) Ricava il path canonico
        //
        let path = if let Some(name) = file_name {
            self.path_from_u16(name)
        } else if let Some(p) = self.path_of(file_context.ino) {
            p.to_string_lossy().to_string()
        } else {
            eprintln!("[ERROR] cleanup: file_name assente e ino non trovato");
            return;
        };

        let rel = RemoteFs::rel_of(std::path::Path::new(&path));

        if rel == "." {
            eprintln!("[ERROR] cleanup: impossibile cancellare la root directory");
            return;
        }

        //se non hai i permessi negi la cancellazione
        if let Some(attr) = self.get_attr_cache(&PathBuf::from(&rel)) {
            if (attr.perm & 0o222) == 0 {
                println!(
                    "[SET_DELETE] denied by perm for {:?} perm={:#o}",
                    path, attr.perm
                );
                //return Err(FspError::WIN32(ERROR_ACCESS_DENIED));
                return;
            }
        }

        //
        // 2) Determina parent e nome
        //
        let parent_rel = std::path::Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());

        let name_only = std::path::Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        let parent_path = PathBuf::from(&parent_rel);
        println!("[CLEANUP] ParentPath : {:?} ", parent_path);

        // Se c'è un TempWrite pendente per questo ino, non evictare né cancellare
        if self
            .state
            .writes
            .lock()
            .unwrap()
            .contains_key(&file_context.ino)
        {
            println!(
                "[CLEANUP] skip: pending TempWrite for ino {}",
                file_context.ino
            );
            return;
        }

        //
        // 3) Usa dir_entries() con cache invece di api.ls()
        //
        let list = match self.dir_entries(&parent_path) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "[ERROR] cleanup: dir_entries fallita su '{}': {}",
                    parent_rel, e
                );
                return;
            }
        };

        //
        // 4) Se l’entry non esiste già localmente o nel backend → solo evict
        //
        let Some((_, de)) = list.iter().find(|(_, d)| d.name == name_only) else {
            println!("[CLEANUP] entry '{}' già sparita, eseguo solo evict", rel);
            self.evict_all_state_for(&path);
            self.evict_all_state_for(&parent_path.to_string_lossy());
            return;
        };

        let is_dir = RemoteFs::is_dir(&de);

        //
        // 5) Verifica flag delete-on-close
        //mettiamo il flag sempre attivo FSP_CLEANUP_DELETE per forzare la cancellazione
        let del_flag = (flags & (FspCleanupDelete as u32)) != 0;
        //let del_flag = (flags & (FSP_CLEANUP_DELETE as u32)) != 0;
        let del_ctx = file_context.delete_on_close.load(Ordering::Relaxed);

        println!(
            "[CLEANUP] rel='{}' is_dir={} del_flag={} del_ctx={}",
            rel, is_dir, del_flag, del_ctx
        );

        if !(del_flag || del_ctx) {
            println!("[DEBUG] cleanup: no delete request, skip");
            return;
        }

        //
        // 6) Per directory: controlla se è vuota (CanDelete dovrebbe averlo garantito)
        //
        if is_dir {
            let dir_path = PathBuf::from(&rel);

            match self.dir_entries(&dir_path) {
                Ok(children) => {
                    if !children.is_empty() {
                        eprintln!(
                            "[ERROR] cleanup: dir '{}' non vuota al momento del delete",
                            rel
                        );
                        return;
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[ERROR] cleanup: dir_entries su dir '{}' fallita: {}",
                        rel, e
                    );
                    return;
                }
            }
        }

        //
        // 7) Esegui la delete lato backend
        //
        match self.rt.block_on(self.api.delete(&rel)) {
            Ok(_) => println!("[DEBUG] cleanup: '{}' eliminato", rel),
            Err(e) => {
                eprintln!("[ERROR] cleanup: delete '{}' fallita: {}", rel, e);
                return;
            }
        }

        //
        // 8) Aggiorna cache: evict dell’entry eliminata
        //
        self.evict_all_state_for(&path);

        //
        // 9) HARD refresh del parent: chiamata diretta a backend.ls (niente dir_entries) per aggiornale cache al prossimo passsaggio
        /*match self.rt.block_on(self.api.ls(&parent_rel)) {
            Ok(list) => {
                self.insert_dir_cache(
                    parent_path.clone(),
                    (list.clone(), std::time::SystemTime::now())
                );
                println!("[CLEANUP] parent '{:?}' hard-refreshed", parent_path);
            }
            Err(e) => {
                eprintln!("[ERROR] cleanup: ls parent '{}' fallita: {}, invalido cache", parent_rel, e);
                // invalida per forzare miss al prossimo accesso
                self.dir_cache.lock().unwrap().remove(&parent_path);
            }
        }*/
        let _ = self.update_cache(&parent_path);

        println!("[CLEANUP] done '{}'", rel);
    }
}

/*pub fn mount_fs(mountpoint: &str, api: FileApi) -> anyhow::Result<()> {
let rt = Arc::new(Runtime::new()?);
let fs = RemoteFs::new(api, rt);
fs.init_cache();*/

pub fn mount_fs(mountpoint: &str, api: FileApi, url: String) -> anyhow::Result<()> {
    let rt = Arc::new(Runtime::new()?);
    let fs = RemoteFs::new(api, rt.clone());
    let fs_state = fs.state.clone(); // ← CRITICO: Clona lo stato per il WebSocket

    fs.init_cache();

    // Configura WinFSP
    let mut vparams = VolumeParams::default();
    vparams.sectors_per_allocation_unit(64);
    vparams.sector_size(4096);
    vparams.file_info_timeout(5);
    vparams.case_sensitive_search(true);
    vparams.case_preserved_names(true);
    vparams.unicode_on_disk(true);
    vparams.pass_query_directory_filename(true);

    let mut host = FileSystemHost::new(vparams, fs)?;
    host.mount(mountpoint)?;
    host.start()?;

    // ✅ Avvia WebSocket listener in background
    println!("[Mount] Starting WebSocket listener for: {}", url);
    {
        let url_clone = url.clone();
        rt.spawn(async move {
            start_websocket_listener(&url_clone, fs_state);
        });
    }

    // Attendi Ctrl-C
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        println!("\n[Mount] Ricevuto segnale Ctrl-C, terminazione...");
        r.store(false, Ordering::SeqCst);
    })?;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!(
        "║  Filesystem montato su: {}                         ",
        mountpoint
    );
    println!("║  Backend URL: {}                                   ", url);
    println!("║  WebSocket attivo per notifiche real-time                 ║");
    println!("║                                                            ║");
    println!("║  Premi Ctrl-C per smontare e uscire                       ║");
    println!("║  Premi F5 in Explorer per aggiornare dopo modifiche       ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    while running.load(Ordering::SeqCst) {
        thread::sleep(std::time::Duration::from_millis(100));
    }

    println!("[Mount] Smonto il filesystem...");
    Ok(())
}
