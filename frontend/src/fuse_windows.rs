use ctrlc;
use std::collections::{HashMap,HashSet};
use std::fs::FileType;
use std::io::{self, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
//use std::time::SystemTime;
use tokio::runtime::Runtime;
use winfsp::filesystem::{DirMarker, FileInfo, FileSecurity, FileSystemContext, OpenFileInfo,DirBuffer};
use winfsp::host::{FileSystemHost, VolumeParams};
use winfsp::{FspError, Result as WinFspResult};
use std::{ffi::c_void, ptr};
use widestring::{U16CStr, U16CString};
// API Windows per convertire SDDL -> SECURITY_DESCRIPTOR (self-relative)
use windows_sys::Win32::Foundation::{LocalFree, HLOCAL, ERROR_ALREADY_EXISTS,ERROR_ACCESS_DENIED, ERROR_FILE_NOT_FOUND, ERROR_INVALID_PARAMETER,
    ERROR_NOT_SAME_DEVICE, ERROR_DIRECTORY, ERROR_NOT_SUPPORTED,};
use windows_sys::Win32::Security::Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW;
use windows_sys::Win32::Storage::FileSystem::{FILE_WRITE_DATA,FILE_ATTRIBUTE_DIRECTORY,FILE_ATTRIBUTE_NORMAL,FILE_FLAG_BACKUP_SEMANTICS,FILE_FLAG_OPEN_REPARSE_POINT,FILE_ATTRIBUTE_READONLY,FILE_ATTRIBUTE_HIDDEN, FILE_ATTRIBUTE_SYSTEM,
    FILE_ATTRIBUTE_ARCHIVE,};
//use windows_sys::Win32::System::IO::CREATE_DIRECTORY;
use winfsp_sys::FspCleanupDelete;



use winfsp_sys::{FspFileSystemAddDirInfo, FSP_FSCTL_DIR_INFO};
use std::mem::{size_of, zeroed};
use std::ptr::{addr_of_mut};
use std::slice;
//use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};



pub struct MyFileContext {
    pub ino: u64,
    pub temp_write: Option<TempWrite>, // Some se stiamo scrivendo, None se solo lettura
    pub delete_on_close: AtomicBool,
    pub allow_delete: bool,
    pub is_dir: bool,
}
//per la definizione fileAttr di file o directory
#[derive(Clone,Debug)]
enum NodeType { Directory, RegularFile }

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

struct RemoteFs {
    api: FileApi,
    rt: Arc<Runtime>,
    //path <-> ino
    ino_by_path: Mutex<HashMap<PathBuf, u64>>,
    path_by_ino: Mutex<HashMap<u64, PathBuf>>,
    //cache attributi
    dir_cache: Mutex<HashMap<PathBuf,(Vec<DirectoryEntry>, SystemTime)>>,
    attr_cache: Mutex<HashMap<PathBuf, FileAttr>>,
    cache_ttl: Duration,
    writes: Mutex<HashMap<u64, TempWrite>>,
    next_ino: Mutex<u64>,
    already_deleted: Mutex<HashSet<u64>>, // tiene traccia degli inode già cancellati
}

// Costanti WinAPI che non sempre sono re-esportate dal crate
//const FILE_WRITE_DATA: u32 = 0x0002;
const CREATE_DIRECTORY: u32 = 0x00000001;//TODO poi da provare ad usare un import
//const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x00000010;
const FSP_CLEANUP_DELETE: u32 = 0x20;//TODO vedere se si riesce ad importare
const DELETE: u32 = 0x0001_0000;//TODO vedere se si riesce ad importare 


impl RemoteFs {
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
            already_deleted : Mutex::new(HashSet::new()),
            cache_ttl: Duration::from_secs(300),
        }
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
        println!("[CACHEVALID] timestamp :{:?}, cache ttl: {:?}",SystemTime::now().duration_since(timestamp).unwrap(), self.cache_ttl);
        SystemTime::now().duration_since(timestamp).unwrap() < self.cache_ttl
    }

    // Funzione che recupera la cache di una directory
    pub fn get_dir_cache(&self, path: &Path) -> Option<(Vec<DirectoryEntry>, SystemTime)> {
        println!("[GET DIR CACHE] get from path :{:?}", path);
        let cache_entry = self.dir_cache.lock().unwrap().get(path).cloned();
        if let Some((en, ts)) = &cache_entry {
            println!("[GET DIR CACHE] entry : {:?}",en );
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
        // 1) chiave canonica per il parent
        let rel = Self::rel_of(dir);              // "." oppure "./a/b"
        let parent_key = PathBuf::from(rel.clone());

        // 2) backend refresh
        let list = self.rt.block_on(self.api.ls(&rel))?;

        // 3) aggiorna dir_cache con chiave canonica
        {
            let mut dircache = self.dir_cache.lock().unwrap();
            dircache.insert(parent_key.clone(), (list.clone(), SystemTime::now()));
        }

        // 4) aggiorna attr_cache in modo coerente e non aggressivo
        let mut attrcache = self.attr_cache.lock().unwrap();
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
                let ty = if isdir { NodeType::Directory } else { NodeType::RegularFile };
                let perm = Self::parse_perm(&de.permissions);
                let size = if isdir { 0 } else { de.size.max(0) as u64 };
                let attr = self.file_attr(&child, ty, size, Some(de.mtime), perm);
                println!("[INSERT ATTR CACHE] (path , attr) : ({:?}, {:?}) ", child, attr);
                attrcache.insert(child.clone(), attr);
            }
        }

        Ok(())
    }


    // Funzione che inserisce in cache lo stato
    pub fn insert_attr_cache(&self, path: PathBuf, attr: FileAttr) {
        println!("[INSERT ATTR CACHE] (path , attr) : ({:?}, {:?}) ", path, attr);
        self.attr_cache.lock().unwrap().insert(path, attr);
    }

    // Funzione che inserisce in cache lo stato di una directory
    pub fn insert_dir_cache(&self, path: PathBuf, data: (Vec<DirectoryEntry>, SystemTime)) {
        println!("[INSERT DIR CACHE] (path ,data) : ({:?}, {:?}) ", path, data);
        self.dir_cache.lock().unwrap().insert(path, data);
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
        let bytes = unsafe {
            std::slice::from_raw_parts(sd_ptr as *const u8, sd_size as usize).to_vec()
        };
        unsafe { LocalFree(sd_ptr as HLOCAL); }
        Ok(bytes)
    }


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

    fn path_of(&self, ino: u64) -> Option<PathBuf> {
        self.path_by_ino.lock().unwrap().get(&ino).cloned()
    }

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
        if de.is_dir ==1 {
            return true;
        }
        return false;
    }

    fn dir_entries(&self, dir: &Path) -> WinFspResult<Vec<(PathBuf, DirectoryEntry)>> {
        let rel = Self::rel_of(dir);
        //let rel=dir;

        println!(
            "[DEBUG] dir_entries(): chiamata backend -> rel='{}'",
            rel
        );
        //1) prova cache directory
        
        if let Some((entries, ts)) = self.dir_cache.lock().unwrap().get(Path::new(&rel)).cloned() {
            if SystemTime::now()
                .duration_since(ts)
                .unwrap_or(Duration::ZERO)
                < self.cache_ttl
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
        
        match &list_res {//lo uso solo per il print
            Ok(list) => {
                println!(
                    "[DEBUG] dir_entries(): backend OK ({} entries)",
                    list.len()
                );
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
        println!("[DIR_ENTRIES] path utilizzato caso no cache per file attr {}", rel);
        
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
        let mut s = path.to_os_string().to_string_lossy().to_string();

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

    fn evict_all_state_for(&self, path: &str) { //liberi la cache, mapping e temp write
        let path_buf = std::path::PathBuf::from(path);
        if let Some(ino) = self.ino_by_path.lock().unwrap().remove(&path_buf) {
            self.path_by_ino.lock().unwrap().remove(&ino);
            if let Some(tw) = self.writes.lock().unwrap().remove(&ino) {
                let _ = std::fs::remove_file(&tw.tem_path);
            }
        }
        self.attr_cache.lock().unwrap().remove(&path_buf);
    }

    fn can_delete(
        &self,
        file_context: &MyFileContext,
        //file_name: Option<&U16CStr>,
        rel : String,
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
        println!("[CAN_DELETE] accept -> return Ok (WinFsp will signal FspCleanupDelete at Cleanup)");
        Ok(())
    }

    //per trasformare il tempo da u64 a Systime
    fn filetime_to_systemtime(ft: u64) -> Option<SystemTime> {
        if ft == 0 { return None; }
        // FILETIME = 100ns ticks since 1601
        let duration = Duration::from_nanos(ft * 100);
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(11644473600) + duration)
    }

    // Helpers locali
    fn split_parent_name(rel: &str) -> (String, String) {
        let p = Path::new(rel);
        let parent = p.parent()
            .map(|x| x.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let name = p.file_name()
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
        println!("[GET_SECURITY_BY_NAME] start path_abs='{}' rel='{}'", path_abs, rel);

        // SD di default (self-relative), coerente con il resto del FS
        // Nota: usa la tua helper sd_from_sddl; se fallisce, fallback a SD nullo valido.
        let sd_bytes = RemoteFs::sd_from_sddl("O:WDG:WD D:(A;;FA;;;WD)")
            .unwrap_or_else(|_| Vec::new());
        let required = sd_bytes.len();
        println!("[GET_SECURITY_BY_NAME] sd_required={} bytes", required);

        // Gestione buffer SD: non scrivere quando insufficiente; esegui la copia solo se capiente
        if let Some(buff) = buf {
            let cap = buff.len();
            if cap < required {
                println!(
                    "[GET_SECURITY_BY_NAME] SD buffer too small: cap={} need={}",
                    cap, required
                );
                // Non scrivere nulla nel buffer insufficiente, ritorna solo la size insieme ai meta.
                let attrs = if is_root { FILE_ATTRIBUTE_DIRECTORY } else { FILE_ATTRIBUTE_NORMAL };
                return Ok(FileSecurity {
                    reparse: false,
                    attributes: attrs,
                    sz_security_descriptor: required as u64,
                });
            } else if required > 0 {
                unsafe {
                    let dst = buff.as_mut_ptr() as *mut u8;
                    std::ptr::copy_nonoverlapping(sd_bytes.as_ptr(), dst, required);
                }
                println!(
                    "[GET_SECURITY_BY_NAME] SD copied: {} bytes (first16={:02x?})",
                    required,
                    &sd_bytes[..sd_bytes.len().min(16)]
                );
            }
        }

        // Root: esiste sempre
        if is_root {
            println!("[GET_SECURITY_BY_NAME] root -> attrs=DIR");
            return Ok(FileSecurity {
                reparse: false,
                attributes: FILE_ATTRIBUTE_DIRECTORY,
                sz_security_descriptor: required as u64,
            });
        }

        // Determina parent e nome, mantenendo la forma canonica “.”/“./sub/...”
        let parent_rel = Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let name_only = std::path::Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        // Chiave coerente per la cache/back-end
        let parent_path = PathBuf::from(&parent_rel);
        println!(
            "[GET_SECURITY_BY_NAME] parent_rel='{}' name='{}' parent_key='{}'",
            parent_rel, name_only, parent_path.display()
        );

        // Usa dir_entries (cache-aware)
        let list = match self.dir_entries(&parent_path) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[ERROR] get_security_by_name: dir_entries('{}') failed: {}", parent_rel, e);
                return Err(e);
            }
        };
        println!("[GET_SECURITY_BY_NAME] parent entries: count={}", list.len());

        // Cerca l’entry per nome
        if let Some((child_path, de)) = list.iter().find(|(_, d)| d.name == name_only) {
            let is_dir = RemoteFs::is_dir(&de);
            let attrs = if is_dir { FILE_ATTRIBUTE_DIRECTORY } else { FILE_ATTRIBUTE_NORMAL };

            // Alloca ino se manca (non fa male se già presente)
            let _ = self.alloc_ino(std::path::Path::new(&path_abs));

            println!(
                "[GET_SECURITY_BY_NAME] FOUND '{}' (is_dir={}) -> attrs={:#x}, sd_len={}",
                child_path.display(), is_dir, attrs, required
            );

            return Ok(FileSecurity {
                reparse: false,
                attributes: attrs,
                sz_security_descriptor: required as u64,
            });
        }

        // Non trovato
        eprintln!(
            "[ERROR] get_security_by_name: '{}' NOT FOUND in parent '{}'",
            name_only, parent_rel
        );
        Err(FspError::WIN32(
            windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND,
        ))
    }


    fn get_security(
        &self,
        context: &Self::FileContext,
        security_descriptor: Option<&mut [c_void]>,
    ) -> WinFspResult<u64> {
        println!("siamo in get security");
        // 1) Costruisci un SD self-relative valido. Qui usiamo un DACL nulla (consistente con il tuo get_security precedente).
        //    Se vuoi uno SDDL specifico (es. "O:WDG:WD..."), puoi sostituire con sd_from_sddl(...) già presente nel file.
        //    Esempio alternativo:
        let sd_bytes = Self::sd_from_sddl("O:WDG:WDD:(A;;FA;;;WD)").unwrap_or_else(|_| Vec::new());
        /*let sd = SecurityDescriptor::new_null_dacl()
            .map_err(|e| FspError::from(e))?;
        let sd_bytes = sd.as_bytes(); // self-relative bytes*/

        // 2) Se il chiamante chiede solo la dimensione (buffer None), ritorna la size richiesta.
        if security_descriptor.is_none() {
            return Ok(sd_bytes.len() as u64);
        }

        // 3) Se c’è un buffer, verifica capienza e copia i bytes dell’SD.
        //    security_descriptor è &mut [c_void]; reinterpretalo come &mut [u8].
        let buf_void = security_descriptor.unwrap();
        let buf_len = buf_void.len();
        let dst_u8: &mut [u8] = unsafe {
            slice::from_raw_parts_mut(buf_void.as_mut_ptr() as *mut u8, buf_len)
        };

        if dst_u8.len() < sd_bytes.len() {
            // Buffer insufficiente: comunica la size necessaria con ERROR_INSUFFICIENT_BUFFER.
            return Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_INSUFFICIENT_BUFFER));
        }

        dst_u8[..sd_bytes.len()].copy_from_slice(&sd_bytes);

        // 4) Ritorna la size effettiva dell’SD copiato.
        Ok(sd_bytes.len() as u64)
    }


    fn get_file_info(
        &self,
        context: &MyFileContext,
        file_info: &mut FileInfo,
    ) -> WinFspResult<()> {
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
        println!("[GET_FILE_INFO] path_abs='{}' rel='{}'", path.display(), rel);

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

                println!(
                    "[GET_FILE_INFO] dir fallback hit: mt={:?} nt={:#x}",
                    t, nt
                );
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
            let ty = if isdir { NodeType::Directory } else { NodeType::RegularFile };
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

        // 2) Parent/nome canonici
        let (src_parent_rel, src_name) = RemoteFs::split_parent_name(&src_rel);
        let (dst_parent_rel, dst_name) = RemoteFs::split_parent_name(&dst_rel);
        let src_parent_key = std::path::PathBuf::from(&src_parent_rel);
        let dst_parent_key = std::path::PathBuf::from(&dst_parent_rel);

        // 3) Liste parent (cache-aware)
        let src_list = self.dir_entries(&src_parent_key)
            .map_err(|e| { eprintln!("[RENAME] dir_entries('{}') failed: {}", src_parent_rel, e); e })?;
        let dst_list = if src_parent_rel == dst_parent_rel {
            src_list.clone()
        } else {
            self.dir_entries(&dst_parent_key)
                .map_err(|e| { eprintln!("[RENAME] dir_entries('{}') failed: {}", dst_parent_rel, e); e })?
        };

        // 4) Sorgente deve esistere
        let (src_child_path, src_de) = match src_list.iter().find(|(_, d)| d.name == src_name) {
            Some((p, d)) => (p.clone(), d.clone()),
            None => {
                eprintln!("[RENAME] source '{}' not found in '{}'", src_name, src_parent_rel);
                return Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND));
            }
        };
        let src_is_dir = RemoteFs::is_dir(&src_de);

        // 5) Gestisci destinazione esistente e replace_if_exists
        if let Some((_, dst_de)) = dst_list.iter().find(|(_, d)| d.name == dst_name) {
            let dst_is_dir = RemoteFs::is_dir(&dst_de);
            if src_is_dir != dst_is_dir {
                eprintln!("[RENAME] type mismatch: src_is_dir={} dst_is_dir={}", src_is_dir, dst_is_dir);
                return Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED));
            }
            if !replace_if_exists {
                eprintln!("[RENAME] destination exists and replace_if_exists=false");
                return Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_ALREADY_EXISTS));
            }
            if dst_is_dir {
                eprintln!("[RENAME] replace directory not supported");
                return Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_NOT_SUPPORTED));
            }
            // Emula REPLACE_EXISTING se hai delete
            if let Err(e) = self.rt.block_on(self.api.delete(&dst_rel)) {
                eprintln!("[RENAME] pre-delete failed for '{}': {}", dst_rel, e);
                return Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED));
            }
        }

        // 6) Backend rename (async PATCH /files/rename?oldRelPath=&newRelPath=)
        if let Err(e) = self.rt.block_on(self.api.rename(&src_rel, &dst_rel)) {
            eprintln!("[RENAME] backend rename failed: {} -> {} err={}", src_rel, dst_rel, e);
            return Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED));
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
                if let Ok(mut byino) = self.path_by_ino.lock() {
                    byino.insert(context.ino, PathBuf::from(dst_abs.clone()));
                }
                if let Ok(mut bypath) = self.ino_by_path.lock() {
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

    

    fn open(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        granted_access: u32,
        open_info: &mut OpenFileInfo,
    ) -> WinFspResult<Self::FileContext> {
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_READONLY,
        };

        // 1) Path Win32 -> rel canonico per cache/backend
        let path = self.path_from_u16(file_name);                       // es. "/nome"
        let rel  = RemoteFs::rel_of(std::path::Path::new(&path));       // "." o "./nome"

        let wants_delete = (granted_access & DELETE) != 0;
        let wants_write = (granted_access & FILE_WRITE_DATA) != 0;
        println!(
            "[OPEN] path='{}' rel='{}' granted_access=0x{:X} create_options=0x{:X} wants_delete={} wants_write={}",
            path, rel, granted_access, create_options, wants_delete,wants_write
        );

        // 2) Root
        if rel == "." {
            let fi = open_info.as_mut();
            fi.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
            fi.file_size = 0;
            // alloc_ino su chiave canonica della root, NON su "/"
            let ino = self.alloc_ino(std::path::Path::new("."));        // FIX
            return Ok(MyFileContext {
                ino,
                is_dir: true,
                allow_delete: wants_delete,
                delete_on_close: AtomicBool::new(false),
                temp_write: None,
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

        println!("[OPEN] Parent per dir_entries :{:?}", parent_key);
        let entries = self.dir_entries(&parent_key)?;                   // cache-aware su chiavi canoniche

        // 4) Trova figlio: child_path è canonico ("./nome")
        let target_name = std::ffi::OsStr::new(&name_only);
        let (child_path, de) = entries
            .into_iter()
            .find(|(p, d): &(PathBuf, DirectoryEntry)| {
                match p.file_name() {
                    Some(n) => n == target_name && d.name == name_only,
                    None => false,
                }
            })
            .ok_or_else(|| {
                eprintln!("[ERROR] open(cache): '{}' not found in parent '{}'", name_only, parent_rel);
                FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND)
            })?;

        let is_dir = RemoteFs::is_dir(&de);

        // 5) alloc_ino sulla chiave canonica del figlio (NON su 'path' Win32)
        let ino = self.alloc_ino(&child_path);                          // FIX

        // 6) Compila OpenFileInfo coerentemente
        let fi = open_info.as_mut();
        if is_dir {
            fi.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
            fi.file_size = 0;
            return Ok(MyFileContext {
                ino,
                is_dir: true,
                allow_delete: wants_delete,                              // non bloccare DELETE/DELETE_CHILD
                delete_on_close: AtomicBool::new(false),
                temp_write: None,

            });
        }

        // 7) File: prova attr_cache; fallback a DirectoryEntry
        if let Some(attr) = self.get_attr_cache(&child_path) {
            let readonly = attr.perm == 0o444;
            fi.file_attributes = if readonly {
                FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_READONLY
            } else {
                FILE_ATTRIBUTE_NORMAL
            };
            fi.file_size         = attr.size;
            fi.creation_time     = RemoteFs::nt_time_from_system_time(attr.crtime);
            fi.last_access_time  = RemoteFs::nt_time_from_system_time(attr.atime);
            fi.last_write_time   = RemoteFs::nt_time_from_system_time(attr.mtime);
            fi.change_time       = RemoteFs::nt_time_from_system_time(attr.ctime);
        } else {
            fi.file_attributes = FILE_ATTRIBUTE_NORMAL;
            fi.file_size       = de.size.max(0) as u64;
            let t  = std::time::UNIX_EPOCH
                .checked_add(std::time::Duration::from_secs(de.mtime as u64))
                .unwrap_or_else(std::time::SystemTime::now);
            let nt = RemoteFs::nt_time_from_system_time(t);
            fi.creation_time     = nt;
            fi.last_access_time  = nt;
            fi.last_write_time   = nt;
            fi.change_time       = nt;
        }

        // 8) TempWrite solo se FILE_WRITE_DATA
        let temp_write = if wants_write {
            let temp_path = self.get_temporary_path(ino);
            if !temp_path.exists() {
                std::fs::File::create(&temp_path).map_err(|e| {
                    let io_err = std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e));
                    FspError::from(io_err)
                })?;
            }
            let tw = TempWrite { tem_path: temp_path, size: 0 };
            self.writes.lock().unwrap().insert(ino, tw.clone());
            Some(tw)
        } else {
            None
        };

        Ok(MyFileContext {
            ino,
            is_dir: false,
            allow_delete: wants_delete,
            delete_on_close: AtomicBool::new(false),
            temp_write,
        })
    }

    fn close(&self, file_context: Self::FileContext) {
        println!("siamo in close");
        let temp_write = match file_context.temp_write {
            Some(tw) => tw,
            None => return,
        };

        let rel_path = RemoteFs::rel_of(&self.path_of(file_context.ino).unwrap());

        if let Err(e) = self.rt.block_on(
            self.api
                .write_file(&rel_path, &temp_write.tem_path.to_string_lossy()),
        ) {
            eprintln!("Errore commit file {}: {:?}", rel_path, e);
        }

        let _ = self.rt.block_on(self.api.chmod(&rel_path, 0o644));

        let parent_rel = Path::new(&rel_path).parent()
            .and_then(|p| Some(p.to_string_lossy().to_string()))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let parent_key = PathBuf::from(parent_rel.clone());

        // a) replace totale (semplice e robusto)
        let _ = self.update_cache(&parent_key);

        let _ = std::fs::remove_file(&temp_write.tem_path);
        self.writes.lock().unwrap().remove(&file_context.ino);
    }

    fn read(
        &self,
        file_context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> WinFspResult<u32> {
        println!("siamo in read");
        let path = self.path_of(file_context.ino).ok_or(FspError::WIN32(1))?;
        let rel_path = RemoteFs::rel_of(&path);
        println!("[READ] rel: {:?}",rel_path);

        let data: Vec<u8> = if let Some(tw) = &file_context.temp_write {
            std::fs::read(&tw.tem_path).map_err(|e| {
                let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                FspError::from(io_err)
            })?
        } else {
            self.rt
                .block_on(self.api.read_file(&rel_path))
                .map_err(|e| {
                    let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                    FspError::from(io_err)
                })?
        };

        let start = offset as usize;
        if start >= data.len() {
            return Ok(0);
        }
        let end = std::cmp::min(start + buffer.len(), data.len());
        let bytes_to_copy = &data[start..end];
        buffer[..bytes_to_copy.len()].copy_from_slice(bytes_to_copy);

        Ok(bytes_to_copy.len() as u32)
    }

    fn write(
        &self,
        file_context: &Self::FileContext,
        buffer: &[u8],
        offset: u64,
        _write_to_end_of_file: bool,
        _constrained_io: bool,
        _file_info: &mut FileInfo,
    ) -> WinFspResult<u32> {
        println!("Siamo in write");
        let tw = match &file_context.temp_write {
            Some(tw) => tw,
            None => return Err(FspError::WIN32(1)), // file opened read-only
        };

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&tw.tem_path)
            .map_err(|e| {
                let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                FspError::from(io_err)
            })?;

        file.seek(std::io::SeekFrom::Start(offset)).map_err(|e| {
            let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
            FspError::from(io_err)
        })?;
        file.write_all(buffer).map_err(|e| {
            let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
            FspError::from(io_err)
        })?;

        Ok(buffer.len() as u32)
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
                (*dir_info_ptr).FileInfo.FileAttributes =
                    if is_dir { FILE_ATTRIBUTE_DIRECTORY } else { FILE_ATTRIBUTE_NORMAL };

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
                let name_dst =
                    (dir_info_ptr as *mut u8).add(core::mem::size_of::<FSP_FSCTL_DIR_INFO>()) as *mut u16;
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

        println!("[CREATE] caso dir parentpath : {}",parent_rel);

        let parent_path=PathBuf::from(&parent_rel);

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
                    });
                }
                Err(e) => {
                    eprintln!("[CREATE] mkdir failed for '{}' -> {}", rel, e);
                    return Err(FspError::from(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
                }
            }
        }

        //Caso File

        // Esiste già --> errore
        if self.backend_entry_exists(&rel) {
            return Err(FspError::WIN32(ERROR_ALREADY_EXISTS));
        }


        let ino = self.alloc_ino(Path::new(&path_str));
        println!("[CREATE] file ino: {:?}",ino);

         // 1. Crea il file temporaneo vuoto PRIMA di chiamare write_file
        let temp_path = self.get_temporary_path(ino);
        if let Err(e) = std::fs::File::create(&temp_path) {
            eprintln!("[CREATE] Errore creazione file temporaneo: {}", e);
            return Err(FspError::WIN32(ERROR_INVALID_PARAMETER as u32));
        }

        //aggiungo la creazione immediata del file vuoto per la gui (di explorer) che mi permette di fare la creazione file
        match self.rt.block_on(self.api.write_file(&rel, &temp_path.to_str().unwrap())) {
            Ok(_) => {
                // 2. Prepara la struttura per le scritture temporanee
                let temp_path = self.get_temporary_path(ino);
                
                // Crea il file temporaneo locale vuoto
                if let Err(e) = std::fs::File::create(&temp_path) {
                    eprintln!("[CREATE] Errore creazione file temporaneo: {}", e);
                    return Err(FspError::WIN32(ERROR_INVALID_PARAMETER as u32));
                }
                //3 Prepara la struttura TempWrite
                let temp_write = TempWrite {
                    tem_path: temp_path,
                    size: 0,
                };
                
                // Salva il riferimento alle scritture temporanee
                self.writes.lock().unwrap().insert(ino, temp_write);
                
                // 4. Crea il contesto del file
                let file_context = MyFileContext {
                    ino,
                    temp_write: Some(TempWrite {
                        tem_path: self.get_temporary_path(ino),
                        size: 0,
                    }),
                    delete_on_close: AtomicBool::new(false),
                    allow_delete: (granted_access & DELETE) != 0,
                    is_dir: false,
                };
                
                fi.file_attributes = FILE_ATTRIBUTE_NORMAL;
                fi.file_size = 0;
                fi.creation_time = nt_time;
                fi.last_access_time = nt_time;
                fi.last_write_time = nt_time;
                fi.change_time = nt_time;

                // 4) Aggiorna cache parent: ricarica elenco (Explorer leggerà subito)
                let _ = self.update_cache(&parent_path);

                 // ★ NEW: crea attributi file e inserisci in cache (come FUSE create)
                let mut attr = self.file_attr(
                    Path::new(&path_str),
                    NodeType::RegularFile,
                    0,
                    None,
                    0o644,
                );
                attr.nlink = 1;
                self.insert_attr_cache(Path::new(&rel).to_path_buf(), attr);


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
    ) -> WinFspResult<()>
    {
        let path = self.path_of(file_context.ino)
            .ok_or(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND))?;
        let rel = RemoteFs::rel_of(&path);
        let rel_key = PathBuf::from(rel.clone());        // "./file", non "/file"
        let parent_rel = std::path::Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let parent_key = PathBuf::from(parent_rel.clone());

        // 1) attr dalla cache sulla chiave canonica
        let mut attr = if let Some(a) = self.get_attr_cache(&rel_key) {
            a
        } else {
            match self.dir_entries(&parent_key) {
                Ok(entries) => {
                    if let Some((p, de)) = entries.into_iter().find(|(p, _)| *p == rel_key) {
                        let is_dir = Self::is_dir(&de);
                        let ty = if is_dir { NodeType::Directory } else { NodeType::RegularFile };
                        let perm = Self::parse_perm(&de.permissions);
                        let size = if is_dir { 0 } else { de.size.max(0) as u64 };
                        let a = self.file_attr(&p, ty, size, Some(de.mtime), perm);
                        self.insert_attr_cache(p.clone(), a.clone());
                        a
                    } else {
                        return Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND));
                    }
                }
                Err(_) => return Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND)),
            }
        };
        //altri permessi non cambiano l ottale del backend
        // 2) mappa ReadOnly -> chmod backend
        let mode = if (file_attributes & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_READONLY) != 0 {
            0o444
        } else {
            0o644
        };
        self.rt.block_on(self.api.chmod(&rel, mode))
            .map_err(|e| FspError::from(io::Error::new(io::ErrorKind::Other, format!("{}", e))))?;


        // 3) Gestione Timestamps → utimes (equivalente Linux)
        //
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
                attr.ctime = mt; // come la tua setattr Linux
                need_utimes = true;
            }
        }
        
         // Propaga al backend
        if need_utimes {
            self.rt
                .block_on(self.api.utimes(&rel, new_atime, new_mtime))
                .map_err(|e| {
                    FspError::from(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                })?;
        }

        //
        // 4) Aggiorna cache locale (UID/GID/Flags non gestiti su Windows)
        //self.insert_attr_cache(path.clone(), attr.clone());
        let _ = self.update_cache(&parent_key);

        // 5) Aggiorna file_info WinFsp
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

    //equivalente truncate per aumento dimensione di file in write
    fn set_file_size(
        &self,
        file_context: &Self::FileContext,
        new_size: u64,
        _write_to_end_of_file: bool,
        file_info: &mut FileInfo
    ) -> WinFspResult<()> {

        let path = self.path_of(file_context.ino)
            .ok_or(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND))?;
        let rel = RemoteFs::rel_of(&path);

        // 1) Propaga al backend
        self.rt
            .block_on(self.api.truncate(&rel, new_size))
            .map_err(|e| {
                FspError::from(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;

        // 2) Aggiorna cache
        if let Some(mut attr) = self.get_attr_cache(&path) {
            attr.size = new_size;
            attr.blocks = (new_size + 511) / 512;
            attr.mtime = SystemTime::now();
            attr.ctime = attr.mtime;

            self.insert_attr_cache(path.clone(), attr.clone());
        }

        // 3) Aggiorna WinFsp file_info
        file_info.file_size = new_size;

        Ok(())
    }



    
    fn flush(
        &self,
        file_context: std::option::Option<&MyFileContext>,
        _file_info: &mut FileInfo,
    ) -> WinFspResult<()> {
        // Se c'è un temp file, committalo subito
        if let Some(ref tw) = file_context.unwrap().temp_write {
            let path = self.path_of(file_context.unwrap().ino)
                .ok_or(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND))?;
            let rel = RemoteFs::rel_of(&path);

            let parent_rel = std::path::Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
            
            self.rt.block_on(self.api.write_file(&rel, &tw.tem_path.to_string_lossy()))
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

    fn set_delete(&self,
                  file_context: &MyFileContext,
                  file_name: &U16CStr,
                  delete: bool) -> WinFspResult<()> {
         println!(
            "set_delete: delete={} for path={:?}, ino={}",
            delete,
            file_name,
            file_context.ino
        );
        let percorso= Some(file_name);

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
            println!("[CAN_DELETE] no file_name, path_of(ino={}) = {}", file_context.ino, p);
            p
        };

        let rel = RemoteFs::rel_of(std::path::Path::new(&path));

        if delete {
            // Prima verifica se si può cancellare

            self.can_delete(file_context, rel)?;
            
            // 🔴 Importante: marca il contesto come "da cancellare al close"
            file_context.delete_on_close.store(true, Ordering::Relaxed);

            println!("file_context {} marked delete_on_close = true", file_context.ino);
        } else {
            file_context.delete_on_close.store(false, Ordering::Relaxed);
        }

        Ok(())//tornando ok dovrebbe marcare il fspCleanupDelete in modo da abilitare la cancellazione
    }

//problema con la rimozione da windows powershell perchè rmdir non funziona per farlo funzionare bisogna chiamare cmd /c rmdir nome_cartella
    fn cleanup(
        &self,
        file_context: &MyFileContext,
        file_name: Option<&U16CStr>,
        flags: u32,
    ) {
        println!("flag {} e fscClean val: {}", flags, FspCleanupDelete as u32);

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

        //IMPORTANTE Controllareeee
        // Normalizza la path assoluta del parent per dir_entries()
        // questo 90% sbagliato rivedere
       /*  let parent_path = if parent_rel == "." {
            PathBuf::from("/")
        } else {
            PathBuf::from("/").join(&parent_rel)
        };*/
        let parent_path=PathBuf::from(&parent_rel);
        println!("[CLEANUP] ParentPath : {:?} ",parent_path);
        
        // Se c'è un TempWrite pendente per questo ino, non evictare né cancellare
        if self.writes.lock().unwrap().contains_key(&file_context.ino) {
            println!("[CLEANUP] skip: pending TempWrite for ino {}", file_context.ino);
            return;
        }

        //
        // 3) Usa dir_entries() con cache invece di api.ls()
        //
        let list = match self.dir_entries(&parent_path) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[ERROR] cleanup: dir_entries fallita su '{}': {}", parent_rel, e);
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
        //
        let del_flag = (flags & (FspCleanupDelete as u32)) != 0;
        let del_ctx  = file_context.delete_on_close.load(Ordering::Relaxed);

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
                    eprintln!("[ERROR] cleanup: dir_entries su dir '{}' fallita: {}", rel, e);
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
        let _=self.update_cache(&parent_path);

        println!("[CLEANUP] done '{}'", rel);
    }

    



}

pub fn mount_fs(mountpoint: &str, api: FileApi) -> anyhow::Result<()> {
    let rt = Arc::new(Runtime::new()?);
    let fs = RemoteFs::new(api, rt);
    fs.init_cache();

    let mut vparams = VolumeParams::default();

    // Layout base
    vparams.sectors_per_allocation_unit(64);    // cluster = 64 * 4096 = 256 KiB [attached_file:21]
    vparams.sector_size(4096);                  // 4 KiB [attached_file:21]
    vparams.file_info_timeout(5);               // seconds [attached_file:21]

    // Sensibilità/preservazione case e Unicode
    vparams.case_sensitive_search(true);
    vparams.case_preserved_names(true);
    vparams.unicode_on_disk(true);

    let mut host = FileSystemHost::new(vparams, fs)?;
    host.mount(mountpoint)?;
    host.start()?;

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        println!("Ricevuto segnale Ctrl-C, terminazione...");
        r.store(false, Ordering::SeqCst);
    })?;

    println!("Filesystem montato. Premi Ctrl-C per smontare e uscire.");
    while running.load(Ordering::SeqCst) {
        thread::sleep(std::time::Duration::from_millis(100));
    }

    println!("Smonto il filesystem e termino.");
    Ok(())
}
