use ctrlc;
use std::collections::HashMap;
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


use winfsp_sys::{FspFileSystemAddDirInfo, FSP_FSCTL_DIR_INFO};
use std::mem::{size_of, zeroed};
use std::ptr::{addr_of_mut};
use std::slice;
//use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};



pub struct MyFileContext {
    pub ino: u64,
    pub temp_write: Option<TempWrite>, // Some se stiamo scrivendo, None se solo lettura
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
}

// Costanti WinAPI che non sempre sono re-esportate dal crate
//const FILE_WRITE_DATA: u32 = 0x0002;
const CREATE_DIRECTORY: u32 = 0x00000001;//poi da provare ad usare un import
//const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x00000010;

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
        // Copia i byte PRIMA di LocalFree
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
            "".to_string()
        } else {
            s.trim_start_matches('/').to_string()
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

    fn is_dir(permissions: &str) -> bool {
        permissions.chars().next().unwrap_or('-') == 'd'
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
        for de in list {
            let child = if rel.is_empty() {
                PathBuf::from("/").join(&de.name)
            } else {
                PathBuf::from("/").join(&rel).join(&de.name)
            };
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
        // path può arrivare con "\" o "/" e con o senza leading slash.
        // Normalizziamo: replace '\' -> '/', garantiamo leading '/'
        let mut p = path.replace('\\', "/");
        if !p.starts_with('/') {
            p = format!("/{}", p);
        }

        // parent come stringa POSIX
        let parent = Path::new(&p)
            .parent()
            .map(|pp| {
                let s = pp.to_string_lossy().to_string().replace('\\', "/");
                if s.is_empty() { "/".to_string() } else { s }
            })
            .unwrap_or_else(|| "/".to_string());

        let name = Path::new(&p)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        match self.rt.block_on(self.api.ls(&parent)) {
            Ok(list) => list.iter().any(|de| de.name == name),
            Err(_) => false,
        }
    }


    fn nt_time_from_system_time(t: SystemTime) -> u64 {
        // NT epoch 1601-01-01 to Unix epoch 1970-01-01 in 100ns ticks
        const SECS_BETWEEN_EPOCHS: u64 = 11644473600;
        match t.duration_since(UNIX_EPOCH) {
            Ok(dur) => (dur.as_secs() + SECS_BETWEEN_EPOCHS) * 10_000_000 + (dur.subsec_nanos() as u64 / 100),
            Err(_) => 0,
        }
    }


    fn is_directory_from_permissions(p: &str) -> bool {
        p.chars().next().unwrap_or('-') == 'd'
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
        // 1) Normalizza path WinFSP -> backend-rel
        let path_abs = self.path_from_u16(name); // es. "\dir\sub" -> "/dir/sub"
        let rel = RemoteFs::rel_of(Path::new(&path_abs)); // "/" -> "", "/dir/sub" -> "dir/sub"
        let is_root = rel.is_empty();

        // 2) Prepara SD: SD Everyone:FA (self-relative); copia solo se buffer capiente
        let sd = RemoteFs::sd_from_sddl("O:WDG:WD D:(A;;FA;;;WD)").unwrap_or_default();
        let sd_len = sd.len();
        if let Some(b) = buf {
            if b.len() >= sd_len {
                unsafe {
                    let dst = std::slice::from_raw_parts_mut(b.as_ptr() as *mut u8, b.len());
                    dst[..sd_len].copy_from_slice(&sd[..sd_len]);
                }
            }
        }

        // 3) Determina esistenza e tipo interrogando il backend
        // Root: esiste sempre ed è directory
        if is_root {
            println!("[DEBUG] get_security_by_name: ROOT path='/'");
            return Ok(FileSecurity {
                reparse: false,
                attributes: FILE_ATTRIBUTE_DIRECTORY, // root è una directory
                sz_security_descriptor: sd_len as u64,
            });
        }

        // Per path non-root, separa parent e nome
        let parent_rel = Path::new(&rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "".to_string()); // parent della "dir/sub" -> "dir", root -> ""
        let name_only = Path::new(&rel)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        // 4) Chiedi ls sul parent e cerca l'entry
        let list = match self.rt.block_on(self.api.ls(&parent_rel)) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[DEBUG] get_security_by_name: ls('{parent_rel}') ERROR: {e}");
                // Se non riesci a interrogare il parent, segnala not found invece di inventare attributi
                return Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND));
            }
        };

        if let Some(de) = list.iter().find(|d| d.name == name_only) {
            // 5) Entry esiste: imposta attributi coerenti al tipo
            let is_dir = RemoteFs::is_dir(&de.permissions);
            let attrs = if is_dir { FILE_ATTRIBUTE_DIRECTORY } else { FILE_ATTRIBUTE_NORMAL };
            println!(
                "[DEBUG] get_security_by_name: FOUND parent='{}' name='{}' is_dir={} sd_len={}",
                parent_rel, name_only, is_dir, sd_len
            );
            return Ok(FileSecurity {
                reparse: false,
                attributes: attrs,
                sz_security_descriptor: sd_len as u64,
            });
        }

        // 6) Entry non esiste: ritorna "not found" per consentire a Windows di chiamare Create
        println!(
            "[DEBUG] get_security_by_name: NOT FOUND parent='{}' name='{}' -> ERROR_FILE_NOT_FOUND",
            parent_rel, name_only
        );
        Err(FspError::WIN32(windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND))
    }



    fn open(
        &self,
        path: &U16CStr,
        _create_options: u32,
        granted_access: u32,
        _file_info: &mut OpenFileInfo,
    ) -> WinFspResult<Self::FileContext> {
        let path = self.path_from_u16(path);
        let ino = self.alloc_ino(Path::new(&path));

        let temp_write = if (granted_access & FILE_WRITE_DATA) != 0 {
            let temp_path = self.get_temporary_path(ino);

            if !temp_path.exists() {
                std::fs::File::create(&temp_path).map_err(|e| {
                    let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                    FspError::from(io_err)
                })?;
            }

            let tw = TempWrite {
                tem_path: temp_path.clone(),
                size: 0,
            };

            self.writes.lock().unwrap().insert(ino, tw.clone());
            Some(tw)
        } else {
            None
        };

        Ok(MyFileContext { ino, temp_write })
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
        println!("[DEBUG] read_directory called for ino={} ({:?})", file_context.ino, self.path_of(file_context.ino));
        let dir_path = self.path_of(file_context.ino).ok_or(FspError::WIN32(1))?;
        let mut entries = self.dir_entries(&dir_path)?;
        let marker_name: Option<String> = marker
            .inner_as_cstr()
            .map(|w: &U16CStr| w.to_string_lossy().to_string());

        println!(
            "[DEBUG] read_directory -> {:?}, marker={:?}, total entries={}",
            dir_path, marker_name, entries.len()
        );

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
                Err(e) => {
                    eprintln!("[DEBUG] Invalid UTF-16 filename {:?}: {}", de.name, e);
                    continue;
                }
            };
            let name_slice = name_w.as_slice(); // senza NUL finale
            let name_len = name_slice.len();

            let fixed = size_of::<FSP_FSCTL_DIR_INFO>();
            let entry_size = (fixed + name_len * 2) as u16;

            let mut raw: [u8; 4096] = [0; 4096];
            if (entry_size as usize) > raw.len() {
                eprintln!("[DEBUG] Entry troppo grande, nome={}", de.name);
                break;
            }

            let dir_info_ptr = raw.as_mut_ptr() as *mut FSP_FSCTL_DIR_INFO;
            unsafe {
                std::ptr::write_bytes(dir_info_ptr as *mut u8, 0, entry_size as usize);

                (*dir_info_ptr).Size = entry_size;

                let is_dir = Self::is_dir(&de.permissions);
                (*dir_info_ptr).FileInfo.FileAttributes =
                    if is_dir { FILE_ATTRIBUTE_DIRECTORY } else { 0 };

                let file_size = de.size as u64;
                (*dir_info_ptr).FileInfo.FileSize = file_size;

                let allocation_unit = 64u64 * 4096u64;
                let alloc = if file_size == 0 {
                    0
                } else {
                    ((file_size + allocation_unit - 1) / allocation_unit) * allocation_unit
                };
                (*dir_info_ptr).FileInfo.AllocationSize = alloc;

                let mtime = UNIX_EPOCH
                    .checked_add(Duration::from_secs(de.mtime as u64))
                    .unwrap_or_else(SystemTime::now);
                let t = RemoteFs::nt_time_from_system_time(mtime);
                (*dir_info_ptr).FileInfo.CreationTime = t;
                (*dir_info_ptr).FileInfo.LastAccessTime = t;
                (*dir_info_ptr).FileInfo.LastWriteTime = t;
                (*dir_info_ptr).FileInfo.ChangeTime = t;

                let name_dst =
                    (dir_info_ptr as *mut u8).add(size_of::<FSP_FSCTL_DIR_INFO>()) as *mut u16;
                std::ptr::copy_nonoverlapping(name_slice.as_ptr(), name_dst, name_len);

                let ok = FspFileSystemAddDirInfo(
                    dir_info_ptr,
                    buffer.as_mut_ptr() as *mut _,
                    buffer.len() as u32,
                    addr_of_mut!(bytes_transferred),
                );

                println!(
                    "[DEBUG]  -> added entry: {:<20} | dir={} | size={} | total={} bytes",
                    de.name, is_dir, file_size, bytes_transferred
                );

                if ok == 0 {
                    println!("[DEBUG]  buffer pieno, stop enumerazione.");
                    break;
                }
            }
        }

        unsafe {
            let _ = FspFileSystemAddDirInfo(
                std::ptr::null_mut(),
                buffer.as_mut_ptr() as *mut _,
                buffer.len() as u32,
                addr_of_mut!(bytes_transferred),
            );
        }

        println!("[DEBUG] read_directory END: {} bytes transferred.\n", bytes_transferred);

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
        let is_dir = (create_options & CREATE_DIRECTORY) != 0;
        let file_info_mut: &mut FileInfo = file_info.as_mut();

        // Intercetta desktop.ini: Windows/Explorer lo crea automaticamente.
        if path_str.to_lowercase().ends_with("desktop.ini") {
            // tenta di creare un file vuoto (ma se fallisce, ignora l'errore per non far fallire l'operazione)
            let _ = self.rt.block_on(self.api.write_file(&path_str, ""));
            file_info_mut.file_attributes = 0;
            file_info_mut.file_size = 0;
            let ino = self.alloc_ino(Path::new(&path_str));
            return Ok(MyFileContext { ino, temp_write: None });
        }

        // Creazione sul backend
        if is_dir {
            if let Err(e) = self.rt.block_on(self.api.mkdir(&path_str)) {
                // Se fallisce, verifica se effettivamente esiste sul backend -> allora ritorna ERROR_ALREADY_EXISTS
                if self.backend_entry_exists(&path_str) {
                    return Err(FspError::WIN32(ERROR_ALREADY_EXISTS as u32));
                } else {
                    let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                    return Err(FspError::from(io_err));
                }
            }
        } else {
            if let Err(e) = self.rt.block_on(self.api.write_file(&path_str, "")) {
                // se fallisce e backend mostra che esiste, segnalalo; altrimenti ritorna l'errore
                if self.backend_entry_exists(&path_str) {
                    return Err(FspError::WIN32(ERROR_ALREADY_EXISTS as u32));
                } else {
                    let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                    return Err(FspError::from(io_err));
                }
            }
        }

        file_info_mut.file_attributes = if is_dir { FILE_ATTRIBUTE_DIRECTORY } else { 0 };
        file_info_mut.file_size = 0;

        if !is_dir && (granted_access & FILE_WRITE_DATA) != 0 {
            return self.open(path, create_options, granted_access, file_info);
        }

        let ino = self.alloc_ino(Path::new(&path_str));
        Ok(MyFileContext { ino, temp_write: None })
    }
}

pub fn mount_fs(mountpoint: &str, api: FileApi) -> anyhow::Result<()> {
    let rt = Arc::new(Runtime::new()?);
    let fs = RemoteFs::new(api, rt);
    let mut vparams = VolumeParams::default();
    vparams.sectors_per_allocation_unit(64);
    vparams.sector_size(4096);
    vparams.file_info_timeout(5);
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
