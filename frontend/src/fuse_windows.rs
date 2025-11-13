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
use windows_sys::Win32::Foundation::{LocalFree, HLOCAL, ERROR_ALREADY_EXISTS};
use windows_sys::Win32::Security::Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW;
use windows_sys::Win32::Storage::FileSystem::{FILE_WRITE_DATA,FILE_ATTRIBUTE_DIRECTORY,FILE_ATTRIBUTE_NORMAL,FILE_FLAG_BACKUP_SEMANTICS,FILE_FLAG_OPEN_REPARSE_POINT};
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
    kind: FileType,
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
    ino_by_path: Mutex<HashMap<PathBuf, u64>>,
    path_by_ino: Mutex<HashMap<u64, PathBuf>>,
    attr_cache: Mutex<HashMap<PathBuf, FileAttr>>,
    writes: Mutex<HashMap<u64, TempWrite>>,
    next_ino: Mutex<u64>,
    already_deleted: Mutex<HashSet<u64>>, // tiene traccia degli inode già cancellati
}

// Costanti WinAPI che non sempre sono re-esportate dal crate
//const FILE_WRITE_DATA: u32 = 0x0002;
const CREATE_DIRECTORY: u32 = 0x00000001;//poi da provare ad usare un import
//const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x00000010;
const FSP_CLEANUP_DELETE: u32 = 0x20;// vedere se si riesce ad importare

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
            writes: Mutex::new(HashMap::new()),
            next_ino: Mutex::new(2),
            already_deleted : Mutex::new(HashSet::new()),
        }
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
        let s = path.to_string_lossy();
        if s == "/" {
            ".".to_string()
        } else {
            format!("./{}", s.trim_start_matches('/'))
        }
    }

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

    fn parse_perm(permissions: &str, is_dir: bool) -> u16 {
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
        let base = ((u << 6) | (g << 3) | o) as u16;
        if is_dir { base | 0o111 } else { base }
    }

    fn is_dir(val:&i64) -> bool {
        if *val ==1 {
            true
        }
        else{
            false
        }
    }

    fn dir_entries(&self, dir: &Path) -> WinFspResult<Vec<(PathBuf, DirectoryEntry)>> {
        let rel = Self::rel_of(dir);

        println!(
            "[DEBUG] dir_entries(): chiamata backend -> rel='{}'",
            rel
        );

        let list_res = self.rt.block_on(self.api.ls(&rel));
        match &list_res {
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

        let mut out = Vec::with_capacity(list.len());
        
        // Normalizza il path base
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
        };
        
        for de in list {
            let child = base_path.join(&de.name);
            println!("[DEBUG] dir_entries(): found {:?}", child);
            out.push((child, de));
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
        file_name: Option<&U16CStr>,
    ) -> WinFspResult<()> {
        println!("[CAN_DELETE] enter");

        // Risolvi path
        let path = if let Some(name) = file_name {
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

        let is_dir = RemoteFs::is_dir(&de.is_dir);
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
        
        let sd = RemoteFs::sd_from_sddl("O:WDG:WD D:(A;;FA;;;WD)")
            .unwrap_or_else(|_| Vec::new());
        let required = sd.len();

       /*  println!(
            "[DEBUG] get_security_by_name: name='{}' rel='{}' is_root={}",
            path_abs, rel, is_root
        );*/

        // Gestione buffer SD (invariato)
        if let Some(buff) = buf {
            let cap = buff.len();
            if cap < required {
                return Ok(FileSecurity {
                    reparse: false,
                    attributes: if is_root { FILE_ATTRIBUTE_DIRECTORY } else { FILE_ATTRIBUTE_NORMAL },
                    sz_security_descriptor: required as u64,
                });
            } else if required > 0 {
                unsafe {
                    let dst = buff.as_mut_ptr() as *mut u8;
                    std::ptr::copy_nonoverlapping(sd.as_ptr(), dst, required);
                }
            }
        }

        // Root esiste sempre
        if is_root {
            return Ok(FileSecurity {
                reparse: false,
                attributes: FILE_ATTRIBUTE_DIRECTORY,
                sz_security_descriptor: required as u64,
            });
        }

        // ✅ FIX: Usa backend_entry_exists che fa ls() correttamente
        let parent_rel = std::path::Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let name_only = std::path::Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        /*println!(
            "[DEBUG] get_security_by_name: parent_rel='{}' name_only='{}'",
            parent_rel, name_only
        );*/

        // ✅ Fai ls() diretto per verificare esistenza
        let list = self.rt.block_on(self.api.ls(&parent_rel))
            .map_err(|e| {
                eprintln!("[ERROR] get_security_by_name: ls failed: {}", e);
                let ioe = std::io::Error::new(std::io::ErrorKind::Other, format!("{e}"));
                FspError::from(ioe)
            })?;

        /*println!(
            "[DEBUG] get_security_by_name: ls returned {} entries",
            list.len()
        );*/
        for de in &list {
            println!("  - name='{}' perms='{}' is_dir={}", de.name, de.permissions, de.is_dir);
        }

        // ✅ Cerca l'entry per nome
        if let Some(de) = list.iter().find(|d| d.name == name_only) {
            let is_dir = RemoteFs::is_dir(&de.is_dir);
            let attrs = if is_dir { 
                FILE_ATTRIBUTE_DIRECTORY 
            } else { 
                FILE_ATTRIBUTE_NORMAL 
            };
            
           /*  println!(
                "[DEBUG] get_security_by_name: FOUND '{}' attrs={:#x}",
                name_only, attrs
            );*/
            
            // ✅ IMPORTANTE: Alloca ino se non esiste già
            let _ = self.alloc_ino(std::path::Path::new(&path_abs));
            
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
        Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND))
    }



    fn open(
        &self,
        file_name: &U16CStr,
        _create_options: u32,
        granted_access: u32,
        open_info: &mut OpenFileInfo,
    ) -> WinFspResult<Self::FileContext> {
        let path = self.path_from_u16(file_name);
        let rel = RemoteFs::rel_of(std::path::Path::new(&path));
        
        println!("[DEBUG] open: path='{}' rel='{}'", path, rel);
        
        // Root directory
        if rel == "." {
            let fi = open_info.as_mut();
            fi.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
            fi.file_size = 0;
            let ino = self.alloc_ino(Path::new(&path));
            return Ok(MyFileContext { ino, temp_write: None, delete_on_close: AtomicBool::new(false), });
        }

        // ✅ Usa la stessa logica di get_security_by_name
        let parent_rel = std::path::Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let name_only = std::path::Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        let list = self.rt.block_on(self.api.ls(&parent_rel))
            .map_err(|e| {
                let ioe = std::io::Error::new(std::io::ErrorKind::Other, format!("{e}"));
                FspError::from(ioe)
            })?;

        let de = list.iter()
            .find(|d| d.name == name_only)
            .ok_or_else(|| {
                eprintln!("[ERROR] open: '{}' not found in '{}'", name_only, parent_rel);
                FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND)
            })?;

        let is_dir = RemoteFs::is_dir(&de.is_dir);
        let ino = self.alloc_ino(Path::new(&path));
        let fi = open_info.as_mut();

        if is_dir {
            fi.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
            fi.file_size = 0;
            return Ok(MyFileContext { ino, temp_write: None, delete_on_close: AtomicBool::new(false), });
        }

        fi.file_attributes = FILE_ATTRIBUTE_NORMAL;
        fi.file_size = de.size as u64;

        // Gestione temp write (invariato)
        let temp_write = if (granted_access & FILE_WRITE_DATA) != 0 {
            let temp_path = self.get_temporary_path(ino);
            if !temp_path.exists() {
                std::fs::File::create(&temp_path).map_err(|e| {
                    let io_err = std::io::Error::new(io::ErrorKind::Other, format!("{}", e));
                    FspError::from(io_err)
                })?;
            }
            let tw = TempWrite { tem_path: temp_path, size: 0 };
            self.writes.lock().unwrap().insert(ino, tw.clone());
            Some(tw)
        } else {
            None
        };

        Ok(MyFileContext { ino, temp_write , delete_on_close: AtomicBool::new(false),})
    }


    fn close(&self, file_context: Self::FileContext) {
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

        let _ = std::fs::remove_file(&temp_write.tem_path);
        self.writes.lock().unwrap().remove(&file_context.ino);
    }

    fn read(
        &self,
        file_context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> WinFspResult<u32> {
        let path = self.path_of(file_context.ino).ok_or(FspError::WIN32(1))?;
        let rel_path = RemoteFs::rel_of(&path);

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
                let is_dir = Self::is_dir(&de.is_dir);
                
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

        // Gestione directory
        if is_dir {
            match self.rt.block_on(self.api.mkdir(&rel)) {
                Ok(_) => {
                    fi.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
                    fi.file_size = 0;
                    fi.creation_time = nt_time;
                    fi.last_access_time = nt_time;
                    fi.last_write_time = nt_time;
                    fi.change_time = nt_time;

                    let ino = self.alloc_ino(Path::new(&path_str));
                    return Ok(MyFileContext { ino, temp_write: None,delete_on_close: AtomicBool::new(false), });
                }
                Err(e) => {
                    if self.backend_entry_exists(&rel) {
                        return self.open(path, create_options, granted_access, file_info);
                    } else {
                        let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                        return Err(FspError::from(io_err));
                    }
                }
            }
        }
        else{

            // Verifica solo che non esista già
            if self.backend_entry_exists(&rel) {
                return Err(FspError::WIN32(ERROR_ALREADY_EXISTS));
            }

            fi.file_attributes = FILE_ATTRIBUTE_NORMAL;
            fi.file_size = 0;
            fi.creation_time = nt_time;
            fi.last_access_time = nt_time;
            fi.last_write_time = nt_time;
            fi.change_time = nt_time;

            let ino = self.alloc_ino(Path::new(&path_str));

            // ✅ Crea file vuoto sul backend SUBITO
            let temp_path = self.get_temporary_path(ino);
            std::fs::File::create(&temp_path).map_err(|e| {
                let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                FspError::from(io_err)
            })?;
            
            // ✅ Scrivi immediatamente file vuoto sul backend che poi tanto può essere riscritto da put
            if let Err(e) = self.rt.block_on(self.api.write_file(&rel, &temp_path.to_string_lossy())) {
                let _ = std::fs::remove_file(&temp_path);
                let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                return Err(FspError::from(io_err));
            }

            // ✅ Prepara temp solo se ha accesso scrittura
            let temp_write = if (granted_access & FILE_WRITE_DATA) != 0 {
                let temp_path = self.get_temporary_path(ino);
                
                // Crea temp locale vuoto
                std::fs::File::create(&temp_path).map_err(|e| {
                    let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                    FspError::from(io_err)
                })?;
                
                let tw = TempWrite { tem_path: temp_path, size: 0 };
                self.writes.lock().unwrap().insert(ino, tw.clone());
                Some(tw)
            } else {
                None
            };

            // ✅ Ritorna il context con temp preparato
            Ok(MyFileContext { ino, temp_write ,delete_on_close: AtomicBool::new(false),})
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
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_READONLY;
        
        let path = self.path_of(file_context.ino)
            .ok_or(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND))?;
        let rel = RemoteFs::rel_of(&path);
        
        // Mappa attributi → permessi Unix
        let mode = if (file_attributes & FILE_ATTRIBUTE_READONLY) != 0 {
            0o444
        } else {
            0o644
        };
        
        // Chiama chmod
        self.rt.block_on(self.api.chmod(&rel, mode))
            .map_err(|e| {
                let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                FspError::from(io_err)
            })?;
        
        // ✅ Aggiorna TUTTI i campi modificati
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
            
            self.rt.block_on(self.api.write_file(&rel, &tw.tem_path.to_string_lossy()))
                .map_err(|e| {
                    let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                    FspError::from(io_err)
                })?;
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

        if delete {
            // Prima verifica se si può cancellare
            self.can_delete(file_context, Some(file_name))?;
            
            // 🔴 Importante: marca il contesto come "da cancellare al close"
            file_context.delete_on_close.store(true, Ordering::Relaxed);

            println!("file_context {} marked delete_on_close = true", file_context.ino);
        } else {
            file_context.delete_on_close.store(false, Ordering::Relaxed);
        }

        Ok(())//tornando ok dovrebbe marcare il fspCleanupDelete in modo da abilitare la cancellazione
    }


    fn cleanup(
        &self,
        file_context: &MyFileContext,
        file_name: Option<&U16CStr>,
        flags: u32,
    ) {
        println!("flag {} e fscClean val: {}", flags, FspCleanupDelete as u32);
        // Esegui solo se marcato delete-on-close da WinFsp
        if (flags & (FspCleanupDelete as u32)) == 0 {
            println!("[DEBUG] cleanup: no DELETE flag, esco");
            return;
        }

        // Risolvi il path canonico (preferisci il nome passato, altrimenti mappa ino->path)
        let path = if let Some(name) = file_name {
            self.path_from_u16(name)
        } else if let Some(p) = self.path_of(file_context.ino) {
            p.to_string_lossy().to_string()
        } else {
            eprintln!("[ERROR] cleanup: file_name assente e ino non trovato");
            return;
        };
        let rel = RemoteFs::rel_of(std::path::Path::new(&path));

        // Non cancellare mai la root
        if rel == "." {
            eprintln!("[ERROR] cleanup: impossibile cancellare la root directory");
            return;
        }

        // Determina parent e nome per interrogare il backend
        let parent_rel = std::path::Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        let name_only = std::path::Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        // Lista il parent per capire se l’entry esiste ancora e se è dir/file
        let list = match self.rt.block_on(self.api.ls(&parent_rel)) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[ERROR] cleanup: ls fallita su '{}': {}", parent_rel, e);
                return;
            }
        };

        // Se già sparita lato backend: ripulisci stato locale e termina
        let Some(de) = list.iter().find(|d| d.name == name_only) else {
            self.evict_all_state_for(&path);
            return;
        };

        let is_dir = RemoteFs::is_dir(&de.is_dir);

        // Per directory: ri-verifica opzionale che sia vuota (CanDelete l’ha già garantito)
        if is_dir {
            match self.rt.block_on(self.api.ls(&rel)) {
                Ok(children) => {
                    if !children.is_empty() {
                        // Non dovrebbe accadere dopo CanDelete; non eliminare se non vuota
                        eprintln!("[ERROR] cleanup: dir '{}' non vuota al momento del delete", rel);
                        return;
                    }
                }
                Err(e) => {
                    eprintln!("[ERROR] cleanup: ls su dir '{}' fallita: {}", rel, e);
                    return;
                }
            }
        }

        // Elimina lato backend
        match self.rt.block_on(self.api.delete(&rel)) {
            Ok(_) => println!("[DEBUG] cleanup: '{}' eliminato", rel),
            Err(e) => {
                eprintln!("[ERROR] cleanup: delete '{}' fallita: {}", rel, e);
                return;
            }
        }

        // Evizione stato locale (mapping, cache e eventuale temp write)
        self.evict_all_state_for(&path);
    }







}

pub fn mount_fs(mountpoint: &str, api: FileApi) -> anyhow::Result<()> {
    let rt = Arc::new(Runtime::new()?);
    let fs = RemoteFs::new(api, rt);

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
