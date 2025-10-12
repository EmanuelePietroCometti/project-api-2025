use anyhow::Result;
use fuser016::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use libc::{EACCES, EEXIST, EINVAL, EIO, ENOENT, ENOTDIR, ENOTEMPTY};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::runtime::Runtime;

use crate::file_api::{DirectoryEntry, FileApi};
const TTL: Duration = Duration::from_secs(1);
const LIST_TTL: Duration = Duration::from_secs(1);
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
}

fn errno_from_anyhow(err: &anyhow::Error) -> i32 {
    use libc::{EACCES, EEXIST, EINVAL, EIO, ENOENT, ENOSPC};
    // 1) Errori I/O locali (file temporanei, fs locale)
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
    // 2) Fallback
    EIO
}

impl RemoteFs {
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
    fn parse_perm(permissions: &str, _is_dir: bool) -> u16 {
        let s = permissions.as_bytes();
        let b = |i| {
            if i < s.len() && s[i] as char != '-' {
                1
            } else {
                0
            }
        };
        let u = (b(1) * 4 + b(2) * 2 + b(3)) as u16;
        let g = (b(4) * 4 + b(5) * 2 + b(6)) as u16;
        let o = (b(7) * 4 + b(8) * 2 + b(9)) as u16;
        ((u << 6) | (g << 3) | o) as u16
    }

    // Funzione che verifica se una i permessi passati corrispondono a quelli di una direcotory
    fn is_dir(permissions: &str) -> bool {
        permissions.chars().next().unwrap_or('-') == 'd'
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
                < LIST_TTL
            {
                let mut out = Vec::with_capacity(entries.len());
                for de in entries {
                    let mut child = PathBuf::from("/");
                    if !rel.is_empty() {
                        child.push(&rel);
                    }
                    child.push(&de.name);
                    let is_dir = Self::is_dir(&de.permissions);
                    let ty = if is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };
                    let perm = Self::parse_perm(&de.permissions, is_dir);
                    let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                    let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm);
                    self.attr_cache.lock().unwrap().insert(child.clone(), attr);
                    out.push((child, de));
                }
                return Ok(out);
            }
        }

        // 2) chiama backend solo se cache scaduta/mancante
        let list = self.rt.block_on(self.api.ls(&rel))?;

        // 3) aggiorna cache directory
        self.dir_cache
            .lock()
            .unwrap()
            .insert(dir.to_path_buf(), (list.clone(), SystemTime::now()));

        // 4) costruisci out e pre‑popola attr_cache per i figli
        let mut out = Vec::with_capacity(list.len());
        for de in list {
            let mut child = PathBuf::from("/");
            if !rel.is_empty() {
                child.push(&rel);
            }
            child.push(&de.name);

            let is_dir = Self::is_dir(&de.permissions);
            let ty = if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let perm = Self::parse_perm(&de.permissions, is_dir);
            let size = if is_dir { 0 } else { de.size.max(0) as u64 };
            let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm);
            self.attr_cache.lock().unwrap().insert(child.clone(), attr);

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
            // Recupera gli attributi correnti
            let parent = path.parent().unwrap_or(Path::new("/"));
            match self.dir_entries(parent) {
                Ok(entries) => {
                    if let Some((_, de)) = entries.into_iter().find(|(p, _)| p == &path) {
                        let is_dir = Self::is_dir(&de.permissions);
                        let ty = if is_dir {
                            FileType::Directory
                        } else {
                            FileType::RegularFile
                        };
                        let perm = Self::parse_perm(&de.permissions, is_dir);
                        let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                        let attr = self.file_attr(&path, ty, size, Some(de.mtime), perm);
                        self.attr_cache.lock().unwrap().insert(path.clone(), attr);
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
        // Applica le modifiche richieste
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
        // Aggiorna la cache
        self.attr_cache
            .lock()
            .unwrap()
            .insert(path.clone(), attr.clone());
        // Risponde con i nuovi attributi
        reply.attr(&TTL, &attr);
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
                    let is_dir = Self::is_dir(&de.permissions);
                    let ty = if is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };
                    let perm = Self::parse_perm(&de.permissions, is_dir);
                    let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                    let attr = self.file_attr(&path, ty, size, Some(de.mtime), perm);
                    self.attr_cache
                        .lock()
                        .unwrap()
                        .insert(path.to_path_buf(), attr);
                    let attr = self.attr_cache.lock().unwrap().get(&path).unwrap().clone();
                    reply.entry(&TTL, &attr, 0);
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
        offset: i64, // usare l'offset del kernel
        mut reply: ReplyDirectory,
    ) {
        let Some(dir) = self.path_of(ino) else {
            reply.error(ENOTDIR);
            return;
        };

        // Recupera lista (meglio da cache)
        let entries = match self.dir_entries(&dir) {
            Ok(v) => v,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // Aggiungi dot entries solo alla prima chiamata
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

        // Calcola indice di partenza dagli offset (3 => primo elemento)
        let mut idx = if offset <= 2 {
            0
        } else {
            (offset - 2) as usize
        };

        // Opzionale: garantire ordine stabile
        // let mut entries = entries; entries.sort_by(|a,b| a.0.cmp(&b.0));

        while idx < entries.len() {
            let (child, de) = &entries[idx];
            let is_dir = Self::is_dir(&de.permissions);
            let ty = if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let child_ino = self.alloc_ino(child);
            let this_off = 3 + idx as i64;
            if !reply.add(child_ino, this_off, ty, child.file_name().unwrap()) {
                break; // buffer pieno: lascia che il kernel riprenda da this_off
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
            reply.attr(&TTL, &attr);
            return;
        }
        let Some(path) = self.path_of(ino) else {
            reply.error(ENOENT);
            return;
        };
        if let Some(attr) = self.attr_cache.lock().unwrap().get(&path).cloned() {
            reply.attr(&TTL, &attr);
            return;
        }
        let parent = path.parent().unwrap_or(Path::new("/"));
        match self.dir_entries(parent) {
            Ok(entries) => {
                if let Some((_, de)) = entries.into_iter().find(|(p, _)| p == &path) {
                    let is_dir = Self::is_dir(&de.permissions);
                    let ty = if is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };
                    let perm = Self::parse_perm(&de.permissions, is_dir);
                    let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                    let mut attr = self.file_attr(&path, ty, size, Some(de.mtime), perm);
                    attr.nlink = if is_dir { 2 } else { 1 };
                    self.attr_cache.lock().unwrap().insert(path.clone(), attr);
                    let attr = self.attr_cache.lock().unwrap().get(&path).unwrap().clone();
                    reply.attr(&TTL, &attr);
                } else {
                    reply.error(ENOENT);
                }
            }
            Err(_) => reply.error(ENOENT),
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
        // recupero stato scrittura temporanea
        let mut writes = self.writes.lock().unwrap();
        let Some(tw) = writes.get_mut(&ino) else {
            reply.error(EIO);
            return;
        };

        // apertura file temporaneo in scrittura
        match std::fs::OpenOptions::new().write(true).open(&tw.tem_path) {
            Ok(mut f) => {
                // posizionamento e scrittura
                if let Err(e) = f
                    .seek(SeekFrom::Start(offset as u64))
                    .and_then(|_| std::io::Write::write_all(&mut f, data))
                {
                    // PermissionDenied => EACCES, altrimenti EIO
                    let errno = if e.kind() == std::io::ErrorKind::PermissionDenied {
                        EACCES
                    } else {
                        EIO
                    };
                    reply.error(errno);
                    return;
                }
            }
            Err(e) => {
                let errno = if e.kind() == std::io::ErrorKind::PermissionDenied {
                    EACCES
                } else {
                    EIO
                };
                reply.error(errno);
                return;
            }
        }

        // aggiorna dimensione stimata e rispondi
        tw.size = tw.size.max((offset as u64) + (data.len() as u64));
        reply.written(data.len() as u32);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        // Nessuna operazione perchè il commit avviene alla chiusura del file (release)
        reply.ok();
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
        // Commit dei dati sul server e rimozione del file temporaneo
        let tw_opt = self.writes.lock().unwrap().remove(&ino);
        if let Some(tw) = tw_opt {
            if let Some(path) = self.path_of(ino) {
                if let Some(parent_path) = path.parent() {
                    self.dir_cache.lock().unwrap().remove(parent_path);
                }
                let rel = Self::rel_of(&path);
                let res = self.rt.block_on(
                    self.api
                        .write_file(&rel, tw.tem_path.to_string_lossy().as_ref()),
                );
                let _ = fs::remove_file(&tw.tem_path);
                match res {
                    Ok(_) => reply.ok(),
                    Err(_) => reply.error(EACCES),
                }
                return;
            }
        }
        reply.ok();
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
                self.dir_cache.lock().unwrap().remove(&parent_path);
                let attr = self.file_attr(&path, FileType::RegularFile, 0, None, 0o644);
                self.attr_cache
                    .lock()
                    .unwrap()
                    .insert(path.clone(), attr.clone());
                let attr = self.attr_cache.lock().unwrap().get(&path).unwrap().clone();
                reply.created(&TTL, &attr, 0, ino, 0);
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
                self.dir_cache.lock().unwrap().remove(&parent_path);
                let mut attr = self.file_attr(&path, FileType::Directory, 0, None, 0o755);
                attr.nlink = 2;
                self.attr_cache
                    .lock()
                    .unwrap()
                    .insert(path.clone(), attr.clone());
                reply.entry(&TTL, &attr, 0);
            }
            Err(_) => reply.error(EEXIST),
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
                self.attr_cache.lock().unwrap().remove(&path);
                self.dir_cache.lock().unwrap().remove(&parent_path);
                if let Some(ino) = self.ino_by_path.lock().unwrap().remove(&path) {
                    self.path_by_ino.lock().unwrap().remove(&ino);
                }
                reply.ok();
            }
            Err(_) => reply.error(ENOENT),
        }
    }

    fn rmdir(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(parent_path) = self.path_of(parent) else {
            reply.error(ENOENT);
            return;
        };
        let path = if parent_path == Path::new("/") {
            PathBuf::from("/").join(name)
        } else {
            parent_path.join(name)
        };
        // verifica tipo via attr_cache/lookup
        let is_dir = if let Some(attr) = self.attr_cache.lock().unwrap().get(&path) {
            matches!(attr.kind, FileType::Directory)
        } else {
            // fallback: prova a leggere le entries; se fallisce, non esiste
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
        // controlla vuotezza
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
                self.attr_cache.lock().unwrap().remove(&path);
                self.dir_cache.lock().unwrap().remove(&path);
                self.dir_cache.lock().unwrap().remove(&parent_path);
                if let Some(ino) = self.ino_by_path.lock().unwrap().remove(&path) {
                    self.path_by_ino.lock().unwrap().remove(&ino);
                }
                reply.ok();
            }
            Err(_) => reply.error(ENOENT),
        }
    }
}

pub fn mount_fs(mountpoint: &str, api: FileApi) -> anyhow::Result<()> {
    let rt = Arc::new(Runtime::new()?);
    let fs = RemoteFs::new(api, rt);
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
