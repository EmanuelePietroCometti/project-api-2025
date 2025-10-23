use ctrlc;
use std::collections::HashMap;
use std::ffi::c_void;
use std::fs::FileType;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::time::SystemTime;
use tokio::runtime::Runtime;
use widestring::U16CStr;
use winfsp::filesystem::{DirMarker, FileInfo, FileSecurity, FileSystemContext, OpenFileInfo};
use winfsp::host::{FileSystemHost, VolumeParams};
use winfsp::security::{AccessMask, SecurityDescriptor};

use winfsp::{FspError, Result as WinFspResult};

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
// Lo usi per cache degli attributi; adattalo se vuoi campi diversi.
#[derive(Clone, Debug)]
struct FileAttr {
    ino: u64,
    size: u64,
    blocks: u64,
    atime: SystemTime,
    mtime: SystemTime,
    ctime: SystemTime,
    crtime: SystemTime,
    // per semplicità uso FileType come "kind"
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
    // path <-> ino
    ino_by_path: Mutex<HashMap<PathBuf, u64>>,
    path_by_ino: Mutex<HashMap<u64, PathBuf>>,
    // cache attributi
    attr_cache: Mutex<HashMap<PathBuf, FileAttr>>,
    // gestione scritture temporanee per file aperti
    writes: Mutex<HashMap<u64, TempWrite>>,
    next_ino: Mutex<u64>,
}

// Costanti WinAPI che non sempre sono re-esportate dal crate
const FILE_WRITE_DATA: u32 = 0x0002;
const CREATE_DIRECTORY: u32 = 0x00000001;
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x00000010;

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
            writes: Mutex::new(HashMap::new()),
            next_ino: Mutex::new(2),
        }
    }
    fn create_access_grant_security() -> FileSecurity {
        // Crea un security descriptor con permessi di accesso completo per tutti
        let mut sec = SecurityDescriptor::new();
        // Configura con permessi permissivi (potresti aver bisogno di impostare specifici controlli)
        // Questa parte dipende dalla API del crate, assicurati di usare i campi corretti
        // Esempio: sec.set_owner(...), sec.set_group(...), sec.set_dacl(...)

        // Se non hai possibilità di impostarlo, puoi semplicemente usare una struct vuota
        // Ricorda: questa soluzione potrebbe non essere sufficiente in tutti i casi
        sec.into()
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

    // Funzione che estrae il path relativo
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

        // su Windows non abbiamo getuid/getgid; usa 0 per default o adattalo
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

    // Funzione che si occupa di trasformare i permessi in formato ottale
    fn parse_perm(permissions: &str, is_dir: bool) -> u16 {
        // Permessi stile "drwxr-xr-x" o "-rw-r--r--"
        // Posizioni 1..=9 map a rwx rwx rwx
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

    // Funzione che verifica se i permessi passati corrispondono a quelli di una directory
    fn is_dir(permissions: &str) -> bool {
        permissions.chars().next().unwrap_or('-') == 'd'
    }

    // Funzione che definisce le entries di una directory
    // Qua dentro avviene la chiamata all'API ls
    fn dir_entries(&self, dir: &Path) -> WinFspResult<Vec<(PathBuf, DirectoryEntry)>> {
        let rel = Self::rel_of(dir);
        // Assumo che api.ls(&rel) -> Result<Vec<DirectoryEntry>, E>
        let list = self.rt.block_on(self.api.ls(&rel)).map_err(|e| {
            // converto l'errore della tua API in FspError
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
            println!(" - Found entry: {:?}", child);
            out.push((child, de));
        }
        Ok(out)
    }

    fn path_from_u16(&self, path: &U16CStr) -> String {
        path.to_os_string().to_string_lossy().to_string()
    }

    // Utility: crea un percorso temporaneo per un ino
    fn get_temporary_path(&self, ino: u64) -> PathBuf {
        // usa la cartella temp di sistema e un nome unico per ino
        let mut p = std::env::temp_dir();
        p.push(format!("remotefs_tmp_{}.bin", ino));
        p
    }
}

impl FileSystemContext for RemoteFs {
    type FileContext = MyFileContext;

    fn get_security_by_name(
        &self,
        name: &U16CStr,
        _buf: Option<&mut [c_void]>,
        _f: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> WinFspResult<FileSecurity> {
        let path_str = self.path_from_u16(name);
        let path = Path::new(&path_str);
        let _ino = self.alloc_ino(path);

        if let Some(sec) = _f(name) {
            Ok(sec)
        } else {
            // Restituisco un FileSecurity di default che permette accesso
            Ok(default_file_security())
        }
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

        // Se abbiamo accesso in scrittura, creiamo un TempWrite
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

            // Inseriamo nella mappa writes
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

        let rel_path = Self::rel_of(&self.path_of(file_context.ino).unwrap());

        // Commit sul backend
        if let Err(e) = self.rt.block_on(
            self.api
                .write_file(&rel_path, &temp_write.tem_path.to_string_lossy()),
        ) {
            eprintln!("Errore commit file {}: {:?}", rel_path, e);
        }

        // Rimuovi file temporaneo
        let _ = std::fs::remove_file(&temp_write.tem_path);

        // Rimuovi dalla mappa writes
        self.writes.lock().unwrap().remove(&file_context.ino);
    }

    fn read(
        &self,
        file_context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> WinFspResult<u32> {
        let path = self.path_of(file_context.ino).ok_or(FspError::WIN32(1))?;
        let rel_path = Self::rel_of(&path);

        // Leggi dal temp file se il file è aperto in scrittura
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
        _file_context: &Self::FileContext,
        _pattern: Option<&U16CStr>,
        mut marker: DirMarker<'_>,
        buffer: &mut [u8],
    ) -> WinFspResult<u32> {
        let dir_path = self.path_of(_file_context.ino).ok_or(FspError::WIN32(1))?;

        let entries = self.dir_entries(&dir_path)?;

        // Trova il punto in cui ripartire rispetto al marker

        let mut offset = 0;

        for (_, de) in entries {
            // Converti il nome in UTF-16LE
            let name_utf16: Vec<u16> = de.name.encode_utf16().chain(Some(0)).collect();
            let name_bytes: &[u8] = unsafe {
                std::slice::from_raw_parts(name_utf16.as_ptr() as *const u8, name_utf16.len() * 2)
            };

            // Se il buffer non ha spazio, interrompi
            if offset + name_bytes.len() > buffer.len() {
                break;
            }

            // Copia il nome nel buffer
            buffer[offset..offset + name_bytes.len()].copy_from_slice(name_bytes);
            offset += name_bytes.len();

            // Non dobbiamo aggiornare il marker manualmente: WinFsp gestisce la ripresa
        }

        Ok(offset as u32)
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
        let path_str = self.path_from_u16(path);
        let is_dir = (create_options & CREATE_DIRECTORY) != 0;
        let file_info_mut: &mut FileInfo = file_info.as_mut();

        // Creazione fisica nel backend remoto
        if is_dir {
            self.rt.block_on(self.api.mkdir(&path_str)).map_err(|e| {
                let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                FspError::from(io_err)
            })?;
        } else {
            self.rt
                .block_on(self.api.write_file(&path_str, ""))
                .map_err(|e| {
                    let io_err = io::Error::new(io::ErrorKind::Other, format!("{}", e));
                    FspError::from(io_err)
                })?;
        }

        file_info_mut.file_attributes = if is_dir { FILE_ATTRIBUTE_DIRECTORY } else { 0 };
        file_info_mut.file_size = 0;

        // Apri il file creato se è un file e se concessi permessi di scrittura
        if !is_dir && (granted_access & FILE_WRITE_DATA) != 0 {
            // Riuso la funzione open che crea il contesto corretto e TempWrite
            return self.open(path, create_options, granted_access, file_info);
        }

        // Per directory o file senza scrittura, ritorna contesto semplice
        let ino = self.alloc_ino(Path::new(&path_str));
        Ok(MyFileContext {
            ino,
            temp_write: None,
        })
    }
}

pub fn mount_fs(mountpoint: &str, api: FileApi) -> anyhow::Result<()> {
    let rt = Arc::new(Runtime::new()?);
    let fs = RemoteFs::new(api, rt);
    let mut vparams = VolumeParams::default();
    vparams.sectors_per_allocation_unit(64); // Numero di settori per unità di allocazione
    vparams.sector_size(4096); // Dimensione di settore (4096 bytes)
    vparams.file_info_timeout(5); // Timeout per caching info file (in secondi);
    let mut host = FileSystemHost::new(vparams, fs)?;
    host.mount(mountpoint)?;
    host.start()?;
    // Creazione di un gestore per il segnale Ctrl-C
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        println!("Ricevuto segnale Ctrl-C, terminazione...");
        r.store(false, Ordering::SeqCst);
    })?;

    // Loop di blocco che si ferma solo dopo Ctrl-C
    println!("Filesystem montato. Premi Ctrl-C per smontare e uscire.");
    while running.load(Ordering::SeqCst) {
        thread::sleep(std::time::Duration::from_millis(100));
    }

    println!("Smonto il filesystem e termino.");
    // Qui puoi inserire eventuali operazioni di cleanup se necessarie

    Ok(())
}
