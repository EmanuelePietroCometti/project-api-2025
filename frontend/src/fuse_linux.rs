use anyhow::Result;
use fuser016::{
    FileAttr, FileType, Filesystem, MountOption, Notifier, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
    spawn_mount2,
};
use futures_util::{SinkExt, StreamExt};
use libc::{EIO, ENOENT, ENOTDIR, ENOTEMPTY};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc::channel},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Runtime;
use tokio::task;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::file_api::{DirectoryEntry, FileApi};
// Tipo leggero per incapsulare status HTTP restando in anyhow::Error
#[derive(Debug, Clone, Copy)]
struct HttpStatus(pub u16);
impl std::fmt::Display for HttpStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "http status {}", self.0)
    }
}
impl std::error::Error for HttpStatus {}
#[derive(Clone)]
pub(crate) struct TempWrite {
    tem_path: PathBuf,
    size: u64,
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
}

struct RemoteFs {
    state: Arc<FsState>,
    api: FileApi,
    rt: Arc<Runtime>,
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
                409 => EEXIST,       // già esiste
                404 => ENOENT,       // non trovato
                401 | 403 => EACCES, // permessi/autorizzazione
                400 => EINVAL,       // argomenti non validi
                507 | 413 => ENOSPC, // spazio insufficiente/too large
                _ => EIO,            // errori server/rete
            };
        }
    }
    EIO
}

fn metadata_from_payload(payload: &Value) -> Option<(PathBuf, String, bool, u64, i64, u16)> {
    let rel = payload["relPath"].as_str()?;
    // name: se manca, proviamo a estrarlo dal path
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

    let abs = Path::new("/").join(rel);

    Some((abs, name, is_dir, size, mtime, perm))
}

pub fn start_websocket_listener(api_url: &str, notifier: Arc<Notifier>, fs_state: Arc<FsState>) {
    let ws_url = api_url.replace("http", "ws") + "/socket.io/?EIO=4&transport=websocket";

    println!("Starting WebSocket listener to {}", ws_url);

    task::spawn(async move {
        println!("Starting WebSocket listener to {}", ws_url);
        let (ws_strem, _) = match connect_async(&ws_url).await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("WebSocket connection error: {:?}", e);
                return;
            }
        };
        println!("WebSocket connected.");
        let (mut write, mut read) = ws_strem.split();
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    println!("WebSocket message received: {}", text);
                    if text.starts_with('0') {
                        println!("Engine.IO open → sending Socket.IO connect (40)");
                        if let Err(e) = write.send(Message::Text("40".into())).await {
                            println!("Failed to send 40 connect: {}", e);
                            break;
                        }
                        continue;
                    }

                    // 2 = Engine.IO ping → rispondi con 3 (pong)
                    if text == "2" {
                        println!("Received ping (2) → sending pong (3)");
                        if let Err(e) = write.send(Message::Text("3".into())).await {
                            println!("Failed to send pong: {}", e);
                            break;
                        }
                        continue;
                    }

                    // 40 = Socket.IO connected
                    if text == "40" {
                        println!("✅ Socket.IO connected to namespace /");
                        continue;
                    }

                    // 42[...] = evento Socket.IO
                    if text.starts_with("42") {
                        println!("📨 Socket.IO event: {}", &text[2..]);

                        let arr: serde_json::Value = match serde_json::from_str(&text[2..]) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("JSON parse error in WebSocket event: {e}");
                                continue;
                            }
                        };

                        let event_name = arr.get(0).and_then(|v| v.as_str()).unwrap_or("");
                        let payload = arr.get(1).unwrap_or(&serde_json::Value::Null);

                        if event_name == "fs_change" {
                            println!("📢 File system change event received: {}", payload);
                            handle_fs_change(payload, &notifier, &fs_state);
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    println!("WebSocket connection closed by server.");
                    break;
                }
                Ok(other) => {
                    println!("WebSocket received non-text message: {:?}", other);
                }
                Err(e) => {
                    eprintln!("WebSocket error: {:?}", e);
                    break;
                }
            }
        }
        println!("WebSocket listener ended.");
    });
}

fn resolve_parent<'a>(path: &'a Path, st: &FsState) -> Option<(u64, &'a std::ffi::OsStr)> {
    let parent = path.parent()?;
    let name = path.file_name()?;
    let ino = st.ino_of(parent)?;
    Some((ino, name))
}

fn handle_fs_change(payload: &serde_json::Value, notifier: &Notifier, fs_state: &FsState) {
    println!("Handle fs_change called");
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
            println!("Unknown fs_change op: {:?}", op);
        }
    }
}

fn handle_created(payload: &Value, notifier: &Notifier, st: &FsState) {
    let Some((abs, name, is_dir, size, mtime, perm)) = metadata_from_payload(payload) else {
        eprintln!("handle_created: invalid metadata in payload: {payload}");
        return;
    };

    // aggiorna la cache interna
    let ino = update_cache_from_metadata(st, &abs, &name, is_dir, size, mtime, perm);

    // invalida il parent nel kernel
    let parent = abs.parent().unwrap_or(Path::new("/"));
    if let Some(parent_ino) = st.ino_of(parent) {
        let _ = notifier.inval_entry(parent_ino, OsStr::new(&name));
        let _ = notifier.inval_inode(parent_ino, 0, 0);
    }

    // invalida anche il file creato
    let _ = notifier.inval_inode(ino, 0, 0);
}


fn handle_deleted_event(payload: &Value, notifier: &Notifier, st: &FsState) {
    if let Some(rel) = payload["relPath"].as_str() {
        let abs = Path::new("/").join(rel);
        handle_deleted_path(&abs, notifier, st);
    } else {
        eprintln!("handle_deleted_event: missing relPath in payload: {payload}");
    }
}

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

fn handle_renamed_event(payload: &Value, notifier: &Notifier, st: &FsState) {
    let Some(old_rel) = payload["oldPath"].as_str() else {
        eprintln!("handle_renamed_event: missing oldPath in payload: {payload}");
        return;
    };
    let Some(new_rel) = payload["newPath"].as_str() else {
        eprintln!("handle_renamed_event: missing newPath in payload: {payload}");
        return;
    };

    let old_abs = Path::new("/").join(old_rel);
    let new_abs = Path::new("/").join(new_rel);

    // 1️⃣ invalida il vecchio parent
    if let Some((old_parent_ino, old_name)) = resolve_parent(&old_abs, st) {
        let _ = notifier.inval_entry(old_parent_ino, old_name);
        let _ = notifier.inval_inode(old_parent_ino, 0, 0);
    }

    // 2️⃣ sposta la mapping path → inode
    let ino = if let Some(ino) = st.ino_of(&old_abs) {
        st.remove_path(&old_abs);
        st.insert_path_mapping(&new_abs, ino);
        ino
    } else {
        // se non lo conoscevamo ancora, usiamo allocate_ino / quello esistente
        st.ino_of(&new_abs).unwrap_or_else(|| st.allocate_ino(&new_abs))
    };

    // 3️⃣ aggiorna gli attributi sul nuovo path usando il metadata
    let Some((_abs_meta, name, is_dir, size, mtime, perm)) = metadata_from_payload(payload) else {
        eprintln!("handle_renamed_event: invalid metadata in payload: {payload}");
        // comunque puliamo gli attr vecchi
        st.remove_attr(&old_abs);
        st.remove_attr(&new_abs);
        return;
    };

    // abs_meta dovrebbe coincidere con new_abs; se no, usiamo comunque new_abs
    let final_abs = &new_abs;

    // update_cache_from_metadata userà l'ino esistente (perché l'abbiamo aggiunto noi sopra)
    let _ = update_cache_from_metadata(st, final_abs, &name, is_dir, size, mtime, perm);

    // 4️⃣ invalida il nuovo parent e l'inode nel kernel
    if let Some((new_parent_ino, _)) = resolve_parent(&new_abs, st) {
        let _ = notifier.inval_inode(new_parent_ino, 0, 0);
    }

    let _ = notifier.inval_inode(ino, 0, 0);
}


fn handle_updated(payload: &Value, notifier: &Notifier, st: &FsState) {
    let Some((abs, name, is_dir, size, mtime, perm)) = metadata_from_payload(payload) else {
        eprintln!("handle_updated: invalid metadata in payload: {payload}");
        return;
    };

    let ino = update_cache_from_metadata(st, &abs, &name, is_dir, size, mtime, perm);

    // invalida l'inode nel kernel (size, mtime, ecc.)
    let _ = notifier.inval_inode(ino, 0, 0);
}


pub fn update_cache_from_metadata(
    st: &FsState,
    abs: &Path,
    name: &str,
    is_dir: bool,
    size: u64,
    mtime: i64,
    perm: u16,
) -> u64 {
    // 1️⃣ Determina il tipo di file
    let kind = if is_dir {
        FileType::Directory
    } else {
        FileType::RegularFile
    };

    // 2️⃣ Determina il parent
    let parent = abs.parent().unwrap_or(Path::new("/"));

    // 3️⃣ Ottieni o crea l'inode
    let ino = match st.ino_of(abs) {
        Some(i) => i,                 // già esiste
        None => st.allocate_ino(abs), // da creare
    };

    // 4️⃣ Costruisci FileAttr coerente con RemoteFs::file_attr
    let blocks = if size == 0 { 0 } else { (size + 511) / 512 };

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
        nlink: if is_dir { 2 } else { 1 },
        uid: 1000,
        gid: 1000,
        rdev: 0,
        flags: 0,
    };

    // 5️⃣ Aggiorna attr_cache
    st.set_attr(abs, attr);

    // 6️⃣ Aggiorna directory padre
    st.insert_child(parent, name.to_string(), ino);

    // 7️⃣ Invalida dir_cache perché non è più aggiornata
    st.remove_dir_cache(parent);

    ino
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
            cache_ttl: Duration::from_secs(300),
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

    pub fn insert_write_tempfile(&self, ino: u64, temp_path: PathBuf) {
        let mut writes = self.writes.lock().unwrap();
        writes.insert(
            ino,
            TempWrite {
                tem_path: temp_path,
                size: 0,
            },
        );
    }

    pub fn update_write_size(&self, ino: u64, delta: u64) {
        let mut writes = self.writes.lock().unwrap();
        if let Some(entry) = writes.get_mut(&ino) {
            entry.size += delta;
        }
    }

    pub fn take_write(&self, ino: u64) -> Option<TempWrite> {
        self.writes.lock().unwrap().remove(&ino)
    }

    pub fn _flush_write(&self, ino: u64) -> Option<TempWrite> {
        self.writes.lock().unwrap().remove(&ino)
    }

    pub fn _remove_write(&self, ino: u64) {
        self.writes.lock().unwrap().remove(&ino);
    }

    pub fn get_write(&self, ino: u64) -> Option<TempWrite> {
        self.writes.lock().unwrap().get(&ino).cloned()
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
}

impl RemoteFs {
    fn get_temporary_path(&self, ino: u64) -> PathBuf {
        let mut tmp_path = std::env::temp_dir();
        tmp_path.push(format!("tempfile_{}", ino));
        tmp_path
    }

    // Funzione che inizializza la cache
    // Viene chiamata all'avvio del filesystem
    pub fn init_cache(&self) {
        self.state.clear_all_cache();
    }

    // Funzione che verifica se la cache è ancora valida
    pub fn is_cache_valid(&self, timestamp: SystemTime) -> bool {
        SystemTime::now().duration_since(timestamp).unwrap() < self.state.cache_ttl
    }

    // Funzione che recupera la cache di una directory
    pub fn get_dir_cache(&self, path: &Path) -> Option<(Vec<DirectoryEntry>, SystemTime)> {
        let cache_entry = self.state.get_dir_cache(&path);
        if let Some((_, ts)) = &cache_entry {
            if !self.is_cache_valid(*ts) {
                return None;
            }
        }
        cache_entry
    }

    pub fn get_attr_cache(&self, path: &Path) -> Option<FileAttr> {
        self.state.get_attr(&path)
    }

    // Funzione che permette di svuotare la cache
    // Se viene passato un path specifico, viene svuotata solo la cache relativa a quel path
    // In caso contrario viene svuotata tutta la cache
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

    // Funzione che effettua l'aggiornamento della cache
    // Viene chiamata dopo operazioni di scrittura, creazione o cancellazione
    pub fn update_cache(&self, dir: &Path) -> anyhow::Result<()> {
        // Forza un refresh dal backend
        let rel = Self::rel_of(dir);
        let list = self.rt.block_on(self.api.ls(&rel))?;
        {
            self.state
                .set_dir_cache(&dir.to_path_buf(), (list.clone(), SystemTime::now()));
        }
        let mut _attrcache = self.state.get_attr(&dir);
        for de in &list {
            let mut child = PathBuf::from("/");
            if !rel.is_empty() {
                child.push(&rel);
            }
            child.push(&de.name);
            let isdir = Self::is_dir(&de);
            let ty = if isdir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let perm = Self::parse_perm(&de.permissions);
            let size = if isdir { 0 } else { de.size.max(0) as u64 };
            let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm);
            self.state.set_attr(&child, attr);
        }
        Ok(())
    }

    // Funzione che inserisce in cache lo stato
    pub fn insert_attr_cache(&self, path: PathBuf, attr: FileAttr) {
        self.state.set_attr(&path, attr);
    }

    // Funzione che inserisce in cache lo stato di una directory
    pub fn insert_dir_cache(&self, path: PathBuf, data: (Vec<DirectoryEntry>, SystemTime)) {
        self.state.set_dir_cache(&path, data);
    }

    // Funzione che instanzia una nuova struct RemoteFs
    fn new(api: FileApi, rt: Arc<Runtime>) -> Self {
        Self {
            state: Arc::new(FsState::new(api.clone(), rt.clone())),
            api,
            rt,
        }
    }
    // Funzione che alloca l'inode
    fn alloc_ino(&self, path: &Path) -> u64 {
        if let Some(ino) = self.state.ino_of(path) {
            ino
        } else {
            self.state.allocate_ino(path)
        }
    }

    // Funzione che recupera il path dall'inode
    fn path_of(&self, ino: u64) -> Option<PathBuf> {
        self.state.path_of(ino)
    }

    // Funzione che estre il path relativo
    fn rel_of(path: &Path) -> String {
        let s = path.to_string_lossy();
        if s == "/" {
            "".to_string()
        } else {
            s.trim_start_matches('/').to_string()
        }
    }

    // Funzione che si occupa di estrapolare i permessi del file
    fn file_attr(
        &self,
        path: &Path,
        ty: FileType,
        size: u64,
        mtime: Option<i64>,
        perm: u16,
    ) -> FileAttr {
        let now = SystemTime::now();
        let mtime_st = mtime
            .and_then(|sec| SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(sec as u64)))
            .unwrap_or(now);
        let uid = unsafe { libc::getuid() } as u32;
        let gid = unsafe { libc::getgid() } as u32;
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
            nlink: if matches!(ty, FileType::Directory) {
                2
            } else {
                1
            },
            uid,
            gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    // Funzione che si occupa di trasformare i permessi in formato ottale
    fn parse_perm(permissions: &str) -> u16 {
        u16::from_str_radix(&permissions, 8).unwrap_or(0)
    }

    // Funzione che verifica se una i permessi passati corrispondono a quelli di una direcotory
    fn is_dir(de: &DirectoryEntry) -> bool {
        if de.is_dir == 1 {
            return true;
        }
        false
    }

    // Funzione che definisce i le entries di una directory
    // Qua dentro avviene la chiamata all'API ls
    pub fn dir_entries(&self, dir: &Path) -> Result<Vec<(PathBuf, DirectoryEntry)>> {
        let rel = Self::rel_of(dir);
        // 1) prova cache directory
        if let Some((entries, ts)) = self.state.get_dir_cache(&dir) {
            if SystemTime::now()
                .duration_since(ts)
                .unwrap_or(Duration::ZERO)
                < self.state.cache_ttl
            {
                let mut out = Vec::with_capacity(entries.len());
                for de in entries {
                    let mut child = PathBuf::from("/");
                    if !rel.is_empty() {
                        child.push(&rel);
                    }
                    child.push(&de.name);
                    let is_dir = Self::is_dir(&de);
                    let ty = if is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };
                    let perm = Self::parse_perm(&de.permissions);
                    let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                    let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm);
                    self.insert_attr_cache(child.clone(), attr);
                    out.push((child, de));
                }
                return Ok(out);
            }
        }

        // 2) chiama backend solo se cache scaduta/mancante
        let list = self.rt.block_on(self.api.ls(&rel))?;

        // 3) aggiorna cache directory
        self.insert_dir_cache(dir.to_path_buf(), (list.clone(), SystemTime::now()));

        // 4) costruisci out e pre-popola attr_cache per i figli
        let mut out = Vec::with_capacity(list.len());
        for de in list {
            let mut child = PathBuf::from("/");
            if !rel.is_empty() {
                child.push(&rel);
            }
            child.push(&de.name);

            let is_dir = Self::is_dir(&de);
            let ty = if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let perm = Self::parse_perm(&de.permissions);
            let size = if is_dir { 0 } else { de.size.max(0) as u64 };
            let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm);
            self.insert_attr_cache(child.clone(), attr);

            out.push((child, de));
        }
        Ok(out)
    }
}

impl Filesystem for RemoteFs {
    // Funzione indispensabile per aggiornare correttmente gli attributi di un file
    // Senza questa funzione non si ha modo di cambiare i permessi e il kernel fallisce (crea il file ma restituisce errore)
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let Some(path) = self.path_of(ino) else {
            reply.error(ENOENT);
            return;
        };
        let parent = path.parent().unwrap_or(Path::new("/"));
        let rel = Self::rel_of(&path);

        // 1) Carica attr di base (da cache o ricaricando il parent)
        let mut attr = if let Some(a) = self.get_attr_cache(&path) {
            a
        } else {
            match self.dir_entries(parent) {
                Ok(entries) => {
                    if let Some((_, de)) = entries.into_iter().find(|(p, _)| p == &path) {
                        let is_dir = Self::is_dir(&de);
                        let ty = if is_dir {
                            FileType::Directory
                        } else {
                            FileType::RegularFile
                        };
                        let perm = Self::parse_perm(&de.permissions);
                        let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                        let a = self.file_attr(&path, ty, size, Some(de.mtime), perm);
                        self.insert_attr_cache(path.clone(), a.clone());
                        a
                    } else {
                        reply.error(ENOENT);
                        return;
                    }
                }
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // 2) Inoltra le modifiche al backend (chmod / truncate / utimes)
        // 2.a) chmod
        if let Some(m) = mode {
            // Propaga i permessi al backend
            match self.rt.block_on(self.api.chmod(&rel, m)) {
                Ok(_) => {
                    attr.perm = (m & 0o777) as u16;
                }
                Err(e) => {
                    reply.error(errno_from_anyhow(&e));
                    return;
                }
            }
        }

        // 2.b) truncate
        if let Some(new_size) = size {
            match self.rt.block_on(self.api.truncate(&rel, new_size)) {
                Ok(_) => {
                    attr.size = new_size;
                    attr.blocks = (new_size + 511) / 512;
                }
                Err(e) => {
                    reply.error(errno_from_anyhow(&e));
                    return;
                }
            }
        }

        // 2.c) utimes (opzionale ma consigliato)
        let mut need_utimes = false;
        let mut new_atime = None;
        let mut new_mtime = None;
        if let Some(a) = atime {
            new_atime = Some(match a {
                TimeOrNow::SpecificTime(t) => t,
                TimeOrNow::Now => SystemTime::now(),
            });
            attr.atime = new_atime.unwrap();
            need_utimes = true;
        }
        if let Some(m) = mtime {
            new_mtime = Some(match m {
                TimeOrNow::SpecificTime(t) => t,
                TimeOrNow::Now => SystemTime::now(),
            });
            let t = new_mtime.unwrap();
            attr.mtime = t;
            attr.ctime = t;
            need_utimes = true;
        }
        if need_utimes {
            // Inoltra anche i nuovi times al backend
            match self
                .rt
                .block_on(self.api.utimes(&rel, new_atime, new_mtime))
            {
                Ok(_) => {}
                Err(e) => {
                    reply.error(errno_from_anyhow(&e));
                    return;
                }
            }
        }

        // 2.d) uid/gid/flags solo locali (se il backend non li supporta)
        if let Some(u) = uid {
            attr.uid = u;
        }
        if let Some(g) = gid {
            attr.gid = g;
        }
        if let Some(f) = flags {
            attr.flags = f;
        }

        // 3) Aggiorna cache e rispondi
        self.insert_attr_cache(path.clone(), attr.clone());
        let _ = self.update_cache(parent);
        reply.attr(&self.state.cache_ttl, &attr);
    }

    // Implementazione minima per far funzionare df
    // Restituisce valori fittizi
    // Non ha impatto sul funzionamento del filesystem
    // Serve per far funzionare correttamente il comando df
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser016::ReplyStatfs) {
        match self.rt.block_on(self.api.statfs()) {
            Ok(stats) => {
                let bsize = stats.bsize; // Dimensione blocco (dal backend)
                let blocks = stats.blocks; // Blocchi totali (dal backend)
                let bfree = stats.bfree; // Blocchi liberi (dal backend)
                let bavail = stats.bavail; // Blocchi disponibili (dal backend)
                let files = stats.files; // Nodi file totali (dal backend)
                let ffree = stats.ffree; // Nodi file liberi (dal backend)
                let namelen: u32 = 255; // Lunghezza massima nome file (hardcoded)
                let frsize: u32 = bsize as u32; // Dimensione frammento

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
            }
            Err(e) => {
                eprintln!(
                    "statfs API call failed: {:?}. Falling back to dummy stats.",
                    e
                );
                let bsize: u32 = 4096;
                let blocks: u64 = 1_000_000;
                let bfree: u64 = 1_000_000;
                let bavail: u64 = 1_000_000;
                let files: u64 = 1_000_000;
                let ffree: u64 = 1_000_000;
                let namelen: u32 = 255;
                let frsize: u32 = bsize;

                reply.statfs(blocks, bfree, bavail, files, ffree, bsize, namelen, frsize);
            }
        }
    }

    // Permette di effettuare la ricerca di una directory per nome e ne resttiuisce il contenuto
    // Non invoca direttamente l'API ls ma lo fa richiamando la funzione dir_entries
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
        let dir = if parent_path == Path::new("/") {
            parent_path.clone()
        } else {
            parent_path
        };
        match self.dir_entries(&dir) {
            Ok(entries) => {
                let target = entries
                    .into_iter()
                    .find(|(p, _)| p.file_name() == Some(name));
                if let Some((path, de)) = target {
                    let is_dir = Self::is_dir(&de);
                    let ty = if is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };
                    let perm = Self::parse_perm(&de.permissions);
                    let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                    let attr = self.file_attr(&path, ty, size, Some(de.mtime), perm);
                    self.insert_attr_cache(path.clone(), attr.clone());
                    reply.entry(&self.state.cache_ttl, &attr, 0);
                } else {
                    reply.error(ENOENT);
                }
            }
            Err(_) => reply.error(ENOENT),
        }
    }
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
            let this_off = 3 + idx as i64;
            if !reply.add(child_ino, this_off, ty, child.file_name().unwrap()) {
                break;
            }
            idx += 1;
        }

        reply.ok();
    }

    // Fondamentale per mantenere sincronizzata e passare dati alla cache
    // Senza questa funzione i dati non sarebbero aggiornati compromettendo il funzionamento di ls
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if ino == 1 {
            let uid = unsafe { libc::getuid() } as u32;
            let gid = unsafe { libc::getgid() } as u32;
            let mut attr = self.file_attr(Path::new("/"), FileType::Directory, 0, None, 0o755);
            attr.uid = uid;
            attr.gid = gid;
            reply.attr(&self.state.cache_ttl, &attr);
            return;
        }

        let Some(path) = self.path_of(ino) else {
            reply.error(ENOENT);
            return;
        };

        let parent = path.parent().unwrap_or(Path::new("/"));

        // Se parent cache è valida, usa attr_cache; altrimenti forza refresh
        let parent_cache_valid = self.get_dir_cache(parent).is_some();
        if parent_cache_valid {
            if let Some(attr) = self.state.get_attr(&path) {
                reply.attr(&self.state.cache_ttl, &attr);
                return;
            }
        }

        // Parent cache non valida o attr mancante -> forza refresh del parent
        match self.dir_entries(parent) {
            Ok(entries) => {
                if let Some((_, de)) = entries.into_iter().find(|(p, _)| p == &path) {
                    let is_dir = Self::is_dir(&de);
                    let ty = if is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };
                    let perm = Self::parse_perm(&de.permissions);
                    let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                    let mut attr = self.file_attr(&path, ty, size, Some(de.mtime), perm);
                    attr.nlink = if is_dir { 2 } else { 1 };
                    self.insert_attr_cache(path.clone(), attr.clone());
                    reply.attr(&self.state.cache_ttl, &attr);
                } else {
                    reply.error(ENOENT);
                }
            }
            Err(_) => reply.error(ENOENT),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let temp_path = self.get_temporary_path(ino);

        // crea fisicamente file vuoto se non esiste
        if !temp_path.exists() {
            if let Err(e) = File::create(&temp_path) {
                eprintln!("Errore nella creazione del file temporaneo: {:?}", e);
                reply.error(libc::EIO);
                return;
            }
        }

        if (flags & libc::O_ACCMODE) != libc::O_RDONLY {
            self.state.insert_write_tempfile(ino, temp_path);
        }

        reply.opened(ino, flags as u32);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
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
        let rel_path = Self::rel_of(&path);

        // Se c'è una scrittura in corso su questo ino, leggi dal temporaneo
        if let Some(tw) = self.state.get_write(ino) {
            // Lettura dal file temporaneo locale
            match File::open(&tw.tem_path) {
                Ok(mut f) => {
                    let mut buf = vec![0u8; size as usize];
                    if let Ok(_) = f.seek(SeekFrom::Start(offset as u64)) {
                        let n = Read::read(&mut f, &mut buf).unwrap_or(0);
                        reply.data(&buf[..n]);
                    } else {
                        reply.error(libc::EIO);
                    }
                }
                Err(_) => reply.error(libc::EIO),
            }
            return;
        }

        // Altrimenti leggi dal backend remoto (Result<Vec<u8>, anyhow::Error>)
        match self.rt.block_on(self.api.read_file(&rel_path)) {
            Ok(data) => {
                let off = offset.max(0) as usize;
                if off >= data.len() {
                    reply.data(&[]);
                    return;
                }
                let end = off.saturating_add(size as usize).min(data.len());
                reply.data(&data[off..end]);
            }
            Err(e) => {
                let errno = errno_from_anyhow(&e);
                reply.error(errno);
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let tw = match self.state.get_write(ino) {
            Some(tw) => tw,
            None => {
                reply.error(libc::EIO);
                return;
            }
        };

        // 2. Apri il file temporaneo
        let res = std::fs::OpenOptions::new().write(true).open(&tw.tem_path);

        match res {
            Ok(mut f) => {
                // 3. Posizionati nel punto corretto
                if f.seek(SeekFrom::Start(offset as u64)).is_err() {
                    reply.error(libc::EIO);
                    return;
                }

                // 4. Scrivi i dati
                if f.write_all(data).is_err() {
                    reply.error(libc::EIO);
                    return;
                }

                // 5. Aggiorna la size in FsState (NON nel clone)
                let new_size = offset as u64 + data.len() as u64;
                self.state
                    .update_write_size(ino, new_size.saturating_sub(tw.size));

                // 6. Rispondi a FUSE
                reply.written(data.len() as u32);
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        // 1. Otteniamo e RIMUOVIAMO la scrittura (se esiste)
        let tw = match self.state.take_write(ino) {
            Some(tw) => tw,
            None => {
                // Nessuna scrittura da flushare → OK
                reply.ok();
                return;
            }
        };

        // 2. Controllo file temporaneo
        if !tw.tem_path.exists() {
            eprintln!("File temporaneo non trovato in flush: {:?}", tw.tem_path);
            reply.error(libc::ENOENT);
            return;
        }

        // 3. Recupero path reale
        let path = match self.path_of(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let rel_path = Self::rel_of(&path);

        // 4. Invio al backend (sincrono attraverso Tokio)
        let result = self.rt.block_on(
            self.api
                .write_file(&rel_path, &tw.tem_path.to_string_lossy()),
        );

        // 5. Risposta a FUSE
        match result {
            Ok(_) => reply.ok(),
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: std::option::Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        // 1. Rimuoviamo l'eventuale buffer di scrittura
        let tw = match self.state.take_write(ino) {
            Some(tw) => tw,
            None => {
                // Nessun dato pendente da commit.
                reply.ok();
                return;
            }
        };

        // 2. Verifica esistenza file temporaneo
        if !tw.tem_path.exists() {
            eprintln!("File temporaneo non trovato in release: {:?}", tw.tem_path);
            reply.error(libc::ENOENT);
            return;
        }

        // 3. Troviamo il path reale
        let path = match self.path_of(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let rel_path = Self::rel_of(&path);

        // 4. Scriviamo sul backend (sincrono via tokio)
        let result = self.rt.block_on(
            self.api
                .write_file(&rel_path, &tw.tem_path.to_string_lossy()),
        );

        // 5. Risposta a FUSE
        match result {
            Ok(_) => reply.ok(),
            Err(_) => reply.error(libc::EIO),
        }
    }

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
        // 1. Trova il percorso del parent
        let parent_path = match self.path_of(parent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // 2. Costruisci il path del nuovo file
        let path = if parent_path == Path::new("/") {
            PathBuf::from("/").join(name)
        } else {
            parent_path.join(name)
        };

        // 3. Alloca inode tramite FsState
        let ino = self.state.allocate_ino(&path);

        // 4. Crea file temporaneo per la scrittura
        let mut tmp = std::env::temp_dir();
        tmp.push(format!("remote_fs_create_{:x}.part", ino));
        let _ = fs::remove_file(&tmp);

        if let Err(e) = fs::File::create(&tmp) {
            eprintln!("create: tmp create failed {:?}: {:?}", tmp, e);
            reply.error(libc::EIO);
            return;
        }

        // 5. Registra il file temporaneo come write buffer IN FsState
        self.state.insert_write_tempfile(ino, tmp.clone());

        // 6. Calcola permessi finali
        let final_mode = mode & !umask;

        // 7. Aggiorna cache del parent (se esistente)
        let _ = self.update_cache(&parent_path);

        // 8. Crea FileAttr interno e aggiornalo nella cache
        let mut attr = self.file_attr(
            &path,
            FileType::RegularFile,
            0,
            None,
            (final_mode & 0o777) as u16,
        );
        attr.nlink = 1;

        self.state.set_attr(&path, attr.clone());

        // 9. Rispondi a FUSE
        reply.created(&self.state.cache_ttl, &attr, 0, ino, 0);
    }

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
        let old = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let new = match newname.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // 1. Recupero path del parent
        let old_parent_path = match self.path_of(parent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let new_parent_path = match self.path_of(newparent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // 2. Costruisco path completi
        let old_path = old_parent_path.join(old);
        let new_path = new_parent_path.join(new);

        // 3. Path relativi per API
        let old_rel = Self::rel_of(&old_path);
        let new_rel = Self::rel_of(&new_path);

        // 4. Chiamata API remota
        match self.rt.block_on(self.api.rename(&old_rel, &new_rel)) {
            Ok(_) => {
                // --- 5. Aggiornamento cache locale ---
                self.clear_cache(Some(&old_path));

                let _ = self.update_cache(&old_parent_path);
                let _ = self.update_cache(&new_parent_path);

                // --- 6. Aggiornamento mapping inode (FsState) ---
                if let Some(ino) = self.state.ino_of(&old_path) {
                    self.state.remove_path(&old_path);
                    self.state.insert_path_mapping(&new_path, ino);
                }

                reply.ok();
            }
            Err(e) => {
                reply.error(errno_from_anyhow(&e));
            }
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        // 1. Recupera percorso del parent
        let parent_path = match self.path_of(parent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // 2. Costruisci il path della directory
        let path = if parent_path == Path::new("/") {
            PathBuf::from("/").join(name)
        } else {
            parent_path.join(name)
        };

        let rel = Self::rel_of(&path);

        // 3. API remota
        match self.rt.block_on(self.api.mkdir(&rel)) {
            Ok(_) => {
                // 4. Aggiorna cache della directory parent
                if let Err(e) = self.update_cache(&parent_path) {
                    eprintln!("update_cache failed after mkdir: {:?}", e);
                    reply.error(EIO);
                    return;
                }

                // 5. Recupera attr se già presente in cache
                if let Some(attr) = self.state.get_attr(&path) {
                    reply.entry(&self.state.cache_ttl, &attr, 0);
                } else {
                    // 6. Crea attr locale
                    let mut attr = self.file_attr(&path, FileType::Directory, 0, None, 0o755);
                    attr.nlink = 2;

                    self.state.set_attr(&path, attr.clone());
                    reply.entry(&self.state.cache_ttl, &attr, 0);
                }
            }
            Err(e) => {
                let errno = errno_from_anyhow(&e);
                reply.error(errno);
            }
        }
    }

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
        let rel = Self::rel_of(&path);
        match self.rt.block_on(self.api.delete(&rel)) {
            Ok(_) => {
                self.clear_cache(Some(&path));
                let _ = self.update_cache(&parent_path);

                self.state.remove_path(&path);
                reply.ok();
            }
            Err(e) => {
                let errno = errno_from_anyhow(&e);
                reply.error(errno);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        // 1. Recupera path del parent
        let parent_path = match self.path_of(parent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // 2. Costruisci path assoluto della directory da eliminare
        let path = if parent_path == Path::new("/") {
            PathBuf::from("/").join(name)
        } else {
            parent_path.join(name)
        };

        // 3. Conferma che esista ed è una directory
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

        // 4. Controlla che la directory sia vuota
        match self.dir_entries(&path) {
            Ok(entries) if entries.is_empty() => {} // ok
            Ok(_) => {
                reply.error(ENOTEMPTY);
                return;
            }
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        }

        // 5. Path relativo da passare alla API remota
        let rel = Self::rel_of(&path);

        // 6. Richiesta al backend
        match self.rt.block_on(self.api.delete(&rel)) {
            Ok(_) => {
                // 7. Aggiorna cache interna
                self.clear_cache(Some(&path));
                let _ = self.update_cache(&parent_path);

                // 8. Aggiorna mapping inode <-> path con FsState
                self.state.remove_path(&path);

                // 9. Risposta a FUSE
                reply.ok();
            }
            Err(e) => {
                let errno = errno_from_anyhow(&e);
                reply.error(errno);
            }
        }
    }
}

pub fn mount_fs(mountpoint: &str, api: FileApi, url: String) -> anyhow::Result<()> {
    let rt = Arc::new(Runtime::new()?);
    let remote_fs = RemoteFs::new(api, rt.clone());
    let fs_state = remote_fs.state.clone();
    remote_fs.init_cache();
    let mp = mountpoint.to_string();
    let options = vec![
        MountOption::FSName("remote_fs".to_string()),
        MountOption::DefaultPermissions,
    ];
    let bg_session = spawn_mount2(remote_fs, &mp, &options).expect("Failed to mount filesystem");
    println!("Mounted remote_fs at {}", mountpoint);
    let notifier = Arc::new(bg_session.notifier());
    {
        let url_clone = url.clone();
        let notifier_clone = notifier.clone();
        rt.spawn(async move {
            println!("Starting WebSocket listener for FS changes...");
            start_websocket_listener(&url_clone, notifier_clone, fs_state);
        });
    }
    let shutting_down = Arc::new(AtomicBool::new(false));
    let (tx, rx) = channel();
    {
        let tx = tx.clone();
        let shutting_down = shutting_down.clone();
        ctrlc::set_handler(move || {
            if !shutting_down.swap(true, Ordering::SeqCst) {
                let _ = tx.send(());
            }
        })
        .expect("Error setting Ctrl-C handler");
    }
    let _ = rx.recv();
    let ok = std::process::Command::new("fusermount")
        .arg("-u")
        .arg(&mountpoint)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !ok {
        let _ = std::process::Command::new("umount")
            .arg("-l")
            .arg(&mountpoint)
            .status();
    }
    let _ = bg_session.join();
    Ok(())
}