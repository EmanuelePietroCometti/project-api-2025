use anyhow::Result;
use fuser016::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use libc::{EIO, ENOENT, ENOTDIR, ENOTEMPTY};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{self},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::runtime::Runtime;

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
struct TempWrite {
    tem_path: PathBuf,
    size: u64,
}
struct RemoteFs {
    api: FileApi,
    rt: Arc<Runtime>,
    // path <-> ino
    ino_by_path: Mutex<HashMap<PathBuf, u64>>,
    path_by_ino: Mutex<HashMap<u64, PathBuf>>,
    // cache attributi
    attr_cache: Mutex<HashMap<PathBuf, FileAttr>>,
    dir_cache: Mutex<HashMap<PathBuf, (Vec<DirectoryEntry>, SystemTime)>>,
    // gestione scritture temporanee per file aperti
    writes: Mutex<HashMap<u64, TempWrite>>,
    next_ino: Mutex<u64>,
    cache_ttl: Duration,
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

impl RemoteFs {
    fn get_temporary_path(&self, ino: u64) -> PathBuf {
        let mut tmp_path = std::env::temp_dir();
        tmp_path.push(format!("tempfile_{}", ino));
        tmp_path
    }

    // Funzione che inizializza la cache
    // Viene chiamata all'avvio del filesystem
    pub fn init_cache(&self) {
        let mut attrcache = self.attr_cache.lock().unwrap();
        let mut dircache = self.dir_cache.lock().unwrap();
        attrcache.clear();
        dircache.clear();
    }

    // Funzione che verifica se la cache è ancora valida
    pub fn is_cache_valid(&self, timestamp: SystemTime) -> bool {
        SystemTime::now().duration_since(timestamp).unwrap() < self.cache_ttl
    }

    // Funzione che recupera la cache di una directory
    pub fn get_dir_cache(&self, path: &Path) -> Option<(Vec<DirectoryEntry>, SystemTime)> {
        let cache_entry = self.dir_cache.lock().unwrap().get(path).cloned();
        if let Some((_, ts)) = &cache_entry {
            if !self.is_cache_valid(*ts) {
                return None;
            }
        }
        cache_entry
    }

    pub fn get_attr_cache(&self, path: &Path) -> Option<FileAttr> {
        self.attr_cache.lock().unwrap().get(path).cloned()
    }

    // Funzione che permette di svuotare la cache
    // Se viene passato un path specifico, viene svuotata solo la cache relativa a quel path
    // In caso contrario viene svuotata tutta la cache
    pub fn clear_cache(&self, path: Option<&Path>) {
        let mut attrcache = self.attr_cache.lock().unwrap();
        let mut dircache = self.dir_cache.lock().unwrap();
        match path {
            Some(p) => {
                attrcache.remove(p);
                dircache.remove(p);
            }
            None => {
                attrcache.clear();
                dircache.clear();
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
            let mut dircache = self.dir_cache.lock().unwrap();
            dircache.insert(dir.to_path_buf(), (list.clone(), SystemTime::now()));
        }
        let mut attrcache = self.attr_cache.lock().unwrap();
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
            attrcache.insert(child.clone(), attr);
        }
        Ok(())
    }

    // Funzione che inserisce in cache lo stato
    pub fn insert_attr_cache(&self, path: PathBuf, attr: FileAttr) {
        self.attr_cache.lock().unwrap().insert(path, attr);
    }

    // Funzione che inserisce in cache lo stato di una directory
    pub fn insert_dir_cache(&self, path: PathBuf, data: (Vec<DirectoryEntry>, SystemTime)) {
        self.dir_cache.lock().unwrap().insert(path, data);
    }

    // Funzione che instanzia una nuova struct RemoteFs
    fn new(api: FileApi, rt: Arc<Runtime>) -> Self {
        let mut ino_by_path = HashMap::new();
        let mut path_by_ino = HashMap::new();
        ino_by_path.insert(PathBuf::from("/"), 1);
        path_by_ino.insert(1, PathBuf::from("/"));
        Self {
            api,
            rt,
            ino_by_path: Mutex::new(ino_by_path),
            path_by_ino: Mutex::new(path_by_ino),
            attr_cache: Mutex::new(HashMap::new()),
            dir_cache: Mutex::new(HashMap::new()),
            writes: Mutex::new(HashMap::new()),
            next_ino: Mutex::new(2),
            cache_ttl: Duration::from_secs(300),
        }
    }
    // Funzione che alloca l'inode
    fn alloc_ino(&self, path: &Path) -> u64 {
        if let Some(ino) = self.ino_by_path.lock().unwrap().get(path).cloned() {
            return ino;
        }
        let mut next_ino = self.next_ino.lock().unwrap();
        let ino = *next_ino;
        *next_ino += 1;
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

    // Funzione che recupera il path dall'inode
    fn path_of(&self, ino: u64) -> Option<PathBuf> {
        self.path_by_ino.lock().unwrap().get(&ino).cloned()
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
        if let Some((entries, ts)) = self.dir_cache.lock().unwrap().get(dir).cloned() {
            if SystemTime::now()
                .duration_since(ts)
                .unwrap_or(Duration::ZERO)
                < self.cache_ttl
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
        _mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
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
        let mut attr = if let Some(attr) = self.attr_cache.lock().unwrap().get(&path).cloned() {
            attr
        } else {
            let parent = path.parent().unwrap_or(Path::new("/"));
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
                        let attr = self.file_attr(&path, ty, size, Some(de.mtime), perm);
                        self.insert_attr_cache(path.clone(), attr.clone());
                        attr
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
        if let Some(m) = _mtime {
            let st = match m {
                TimeOrNow::SpecificTime(t) => t,
                TimeOrNow::Now => SystemTime::now(),
            };
            attr.mtime = st;
            attr.ctime = st;
        }
        if let Some(a) = _atime {
            let st = match a {
                TimeOrNow::SpecificTime(t) => t,
                TimeOrNow::Now => SystemTime::now(),
            };
            attr.atime = st;
        }
        if let Some(u) = uid {
            attr.uid = u;
        }
        if let Some(g) = gid {
            attr.gid = g;
        }
        if let Some(s) = size {
            attr.size = s;
            attr.blocks = (s + 511) / 512;
        }
        if let Some(f) = flags {
            attr.flags = f;
        }
        self.insert_attr_cache(path.clone(), attr.clone());
        reply.attr(&self.cache_ttl, &attr);
    }
    // Implementazione minima per far funzionare df
    // Restituisce valori fittizi
    // Non ha impatto sul funzionamento del filesystem
    // Serve per far funzionare correttamente il comando df
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser016::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 0, 0, 0);
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
                    reply.entry(&self.cache_ttl, &attr, 0);
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
                    .and_then(|p| self.ino_by_path.lock().unwrap().get(p).cloned())
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
            reply.attr(&self.cache_ttl, &attr);
            return;
        }
        let Some(path) = self.path_of(ino) else {
            reply.error(ENOENT);
            return;
        };
        if let Some(attr) = self.attr_cache.lock().unwrap().get(&path).cloned() {
            reply.attr(&self.cache_ttl, &attr);
            return;
        }
        let parent = path.parent().unwrap_or(Path::new("/"));
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
                    reply.attr(&self.cache_ttl, &attr);
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
            if let Err(e) = std::fs::File::create(&temp_path) {
                eprintln!("Errore nella creazione del file temporaneo: {:?}", e);
                reply.error(libc::EIO);
                return;
            }
        }

        if (flags & libc::O_ACCMODE) != libc::O_RDONLY {
            let mut writes = self.writes.lock().unwrap();
            writes.insert(
                ino,
                TempWrite {
                    tem_path: temp_path,
                    size: 0,
                },
            );
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
        let path = self.path_of(ino).unwrap_or_else(|| PathBuf::from(""));

        let rel_path = Self::rel_of(&path);

        // Se hai dato open in write, puoi leggere da file temporaneo
        if let Some(tw) = self.writes.lock().unwrap().get(&ino) {
            let mut file = self
                .rt
                .block_on(tokio::fs::File::open(&tw.tem_path))
                .unwrap();
            self.rt
                .block_on(file.seek(SeekFrom::Start(offset as u64)))
                .unwrap();
            let mut buf = vec![0u8; size as usize];
            let n = self.rt.block_on(file.read(&mut buf)).unwrap();
            buf.truncate(n);
            reply.data(&buf);
            return;
        }

        // Altrimenti leggi dal backend
        let data = self
            .rt
            .block_on(self.api.read_file(&rel_path))
            .unwrap_or_default();
        let end = std::cmp::min((offset as usize) + (size as usize), data.len());
        let slice = &data[offset as usize..end];
        reply.data(slice);
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
        let mut writes = self.writes.lock().unwrap();

        let tw = match writes.get_mut(&ino) {
            Some(tw) => tw,
            None => {
                reply.error(libc::EIO);
                return;
            }
        };

        let res = std::fs::OpenOptions::new().write(true).open(&tw.tem_path);

        match res {
            Ok(mut f) => {
                if f.seek(SeekFrom::Start(offset as u64)).is_err() {
                    reply.error(libc::EIO);
                    return;
                }
                if f.write_all(data).is_err() {
                    reply.error(libc::EIO);
                    return;
                }
                tw.size = tw.size.max(offset as u64 + data.len() as u64);
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
        let writes_guard = self.writes.lock().unwrap();

        if let Some(tw) = writes_guard.get(&ino) {
            if !tw.tem_path.exists() {
                eprintln!("File temporaneo non trovato in release: {:?}", tw.tem_path);
                reply.error(libc::ENOENT);
                return;
            }
            // Calcola percorso relativo corretto per backend
            let path = self.path_of(ino).unwrap();
            let rel_path = Self::rel_of(&path);

            // Esegui commit file temporaneo al backend (sincrono)
            let result = self.rt.block_on(
                self.api
                    .write_file(&rel_path, &tw.tem_path.to_string_lossy()),
            );

            match result {
                Ok(_) => reply.ok(),
                Err(_) => reply.error(libc::EIO),
            }
        } else {
            // Nessun dato da flushare, OK semplice
            reply.ok();
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let mut writes = self.writes.lock().unwrap();

        if let Some(tw) = writes.remove(&ino) {
            if !tw.tem_path.exists() {
                eprintln!("File temporaneo non trovato in release: {:?}", tw.tem_path);
                reply.error(libc::ENOENT);
                return;
            }
            let path = match self.path_of(ino) {
                Some(p) => p,
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };

            // Ottieni percorso relativo per il backend
            let rel_path = Self::rel_of(&path);

            // Tentativo di scrittura file sul backend
            let result = self.rt.block_on(
                self.api
                    .write_file(&rel_path, &tw.tem_path.to_string_lossy()),
            );

            match result {
                Ok(_) => {
                    reply.ok(); // Successo
                }
                Err(e) => {
                    eprintln!(
                        "Errore commit file sul backend release ino {}: {:?}",
                        ino, e
                    );
                    reply.error(libc::EIO); // Errore I/O
                }
            }
        } else {
            // Nessuna scrittura in corso, ok semplice
            reply.ok();
        }
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
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
        let ino = self.alloc_ino(&path);
        let mut tmp = std::env::temp_dir();
        tmp.push(format!("remote_fs_create_{:x}.part", ino));
        let _ = fs::remove_file(&tmp);
        let _ = fs::File::create(&tmp);
        let rel = Self::rel_of(&path);
        match self
            .rt
            .block_on(self.api.write_file(&rel, tmp.to_string_lossy().as_ref()))
        {
            Ok(_) => {
                let _ = self.update_cache(&parent_path);
                let attr = self.file_attr(&path, FileType::RegularFile, 0, None, 0o644);
                self.insert_attr_cache(path.clone(), attr.clone());
                let attr = self.get_attr_cache(&path).unwrap_or(attr);
                reply.created(&self.cache_ttl, &attr, 0, ino, 0);
                let _ = fs::remove_file(&tmp);
            }
            Err(e) => {
                let _ = fs::remove_file(&tmp);
                let errno = errno_from_anyhow(&e);
                reply.error(errno);
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

        match self.rt.block_on(self.api.mkdir(&rel)) {
            Ok(_) => {
                if let Err(e) = self.update_cache(&parent_path) {
                    eprintln!("update_cache failed after mkdir: {:?}", e);
                    reply.error(EIO);
                    return;
                }
                if let Some(attr) = self.get_attr_cache(&path) {
                    reply.entry(&self.cache_ttl, &attr, 0);
                } else {
                    let mut attr = self.file_attr(&path, FileType::Directory, 0, None, 0o755);
                    attr.nlink = 2;
                    self.insert_attr_cache(path.clone(), attr.clone());
                    reply.entry(&self.cache_ttl, &attr, 0);
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

                if let Some(ino) = self.ino_by_path.lock().unwrap().remove(&path) {
                    self.path_by_ino.lock().unwrap().remove(&ino);
                }
                reply.ok();
            }
            Err(e) => {
                let errno = errno_from_anyhow(&e);
                reply.error(errno);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(parent_path) = self.path_of(parent) else {
            reply.error(ENOENT);
            return;
        };
        let path = if parent_path == Path::new("/") {
            PathBuf::from("/").join(name)
        } else {
            parent_path.join(name)
        };
        let is_dir = if let Some(attr) = self.get_attr_cache(&path) {
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
        // esegue delete e pulizia cache/mapping
        let rel = Self::rel_of(&path);
        match self.rt.block_on(self.api.delete(&rel)) {
            Ok(_) => {
                self.clear_cache(Some(&path));
                let _ = self.update_cache(&parent_path);
                if let Some(ino) = self.ino_by_path.lock().unwrap().remove(&path) {
                    self.path_by_ino.lock().unwrap().remove(&ino);
                }
                reply.ok();
            }
            Err(e) => {
                let errno = errno_from_anyhow(&e);
                reply.error(errno);
            }
        }
    }
}

pub fn mount_fs(mountpoint: &str, api: FileApi) -> anyhow::Result<()> {
    let rt = Arc::new(Runtime::new()?);
    let fs = RemoteFs::new(api, rt);
    fs.init_cache();
    let mp = mountpoint.to_string();
    let shutting_down = Arc::new(AtomicBool::new(false)); // Flag atomico per evitare di chiamare più volte lo smontaggio
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    {
        let tx = tx.clone();
        let shutting_down = shutting_down.clone();
        ctrlc::set_handler(move || {
            if !shutting_down.swap(true, Ordering::SeqCst) {
                let _ = tx.send(());
            }
        })?;
    }
    let options = vec![
        MountOption::FSName("remote_fs".into()),
        MountOption::DefaultPermissions,
    ];
    let session = fuser016::spawn_mount2(fs, &mp, &options)?;
    let _ = rx.recv();
    let ok = std::process::Command::new("fusermount3")
        .arg("-u")
        .arg(&mp)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !ok {
        let _ = std::process::Command::new("umount")
            .arg("-l")
            .arg(&mp)
            .status();
    }
    let _ = session.join();
    Ok(())
}
