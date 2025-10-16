 use std::ffi::c_void;
use std::path::PathBuf;
use widestring::U16CStr;
use std::time::Duration;
use winfsp::filesystem::{DirMarker, FileSecurity, FileSystemContext, OpenFileInfo, FileInfo};
use winfsp::host::{FileSystemHost, VolumeParams};

use winfsp::FspError;

pub struct MyFileContext {
    pub ino: u64,
    pub temp_write: Option<TempWrite>, // Some se stiamo scrivendo, None se solo lettura
}

use crate::file_api::{DirectoryEntry, FileApi};
const TTL: Duration = Duration::from_secs(1);

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
    // gestione scritture temporanee per file aperti
    writes: Mutex<HashMap<u64, TempWrite>>,
    next_ino: Mutex<u64>,
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

    // Funzione che verifica se una i permessi passati corrispondono a quelli di una direcotory
    fn is_dir(permissions: &str) -> bool {
        permissions.chars().next().unwrap_or('-') == 'd'
    }

    // Funzione che definisce i le entries di una directory
    // Qua dentro avviene la chiamata all'API ls
    fn dir_entries(&self, dir: &Path) -> Result<Vec<(PathBuf, DirectoryEntry)>> {
        let rel = Self::rel_of(dir);
        let list = self.rt.block_on(self.api.ls(&rel))?;
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
}


impl FileSystemContext for RemoteFs {
    type FileContext = MyFileContext;

    fn get_security_by_name(
        &self,
        _name: &U16CStr,
        _buf: Option<&mut [c_void]>,
        _f: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> Result<FileSecurity, FspError> {
        // Facoltativo: qui potresti ottenere permessi o ACL dal server remoto.
        // Per ora puoi restituire permessi fittizi o "unimplemented!".
        unimplemented!()
    }

    const FILE_WRITE_DATA: u32 = 0x0002;

    fn open(
        &self,
        path: &U16CStr,
        _create_options: u32,
        granted_access: u32,
        _file_info: &mut OpenFileInfo,
    ) -> Result<Self::FileContext, FspError> {
        let path = self.path_from_u16(path);
        let ino = self.alloc_ino(&path);

        // Se abbiamo accesso in scrittura, creiamo un TempWrite
        let temp_write = if granted_access & Self::FILE_WRITE_DATA != 0 {
            let temp_path = self.get_temporary_path(ino);

            if !temp_path.exists() {
                std::fs::File::create(&temp_path)
                    .map_err(|_| FspError::from_win32())?;
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
            self.api.write_file(&rel_path, &temp_write.tem_path.to_string_lossy())// la faccio qua la chiamata API vera e propria della write andando a scrivere il / i file temporanei creati
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
    ) -> Result<u32, FspError> {
        let path = self.path_of(file_context.ino)
            .ok_or(FspError::from_win32())?;
        let rel_path = Self::rel_of(&path);

        // Leggi dal temp file se il file è aperto in scrittura
        let data = if let Some(tw) = &file_context.temp_write {
            std::fs::read(&tw.tem_path).map_err(|_| FspError::from_win32())?
        } else {
            self.rt.block_on(self.api.read_file(&rel_path))
                .map_err(|_| FspError::from_win32())?
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
        _file_context: &Self::FileContext,
        _buffer: &[u8],
        _offset: u64,
        _write_to_end_of_file: bool,
        _constrained_io: bool,
        file_info: &mut FileInfo,
    ) -> Result<u32, FspError> {
        // Quando un file viene scritto:
        // -> invia i byte al backend tramite PUT /files/<path>, ma lo fa solo quando il file viene chiuso
        // TODO: inserire codice asincrono per scrittura remota
        let tw = match &file_context.temp_write {
            Some(tw) => tw,
            None => return Err(FspError::from_win32()), // file opened read-only
        };

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&tw.tem_path)
            .map_err(|_| FspError::from_win32())?;

        file.seek(std::io::SeekFrom::Start(offset))
            .map_err(|_| FspError::from_win32())?;
        file.write_all(buffer)
            .map_err(|_| FspError::from_win32())?;

        Ok(buffer.len() as u32)
    }

    fn read_directory(
        &self,
        _file_context: &Self::FileContext,
        _pattern: Option<&U16CStr>,
        mut _marker: DirMarker<'_>,
        _buffer: &mut [u8],
    ) -> Result<u32, FspError> {
        // Quando viene fatto "dir" o "ls":
        // lo usiamo come wrapper della funzione interna del file system dir_entries la quale chiama effettivamente da backend la ls
        // -> per ogni entry restituita, aggiungi a `marker.add_file(name, attributes)`
       
        let dir_path = self.path_of(_file_context.ino)
                        .ok_or(FspError::from_win32())?;
        
        let entries = self.dir_entries(&dir_path)?;
        
        //trovi il punto in cui si era fermato il marker
        for (path, file_entry) in entries.into_iter().skip_while(|(p, _)| {
           // skip finché il path è <= marker
            marker.file_name().map_or(false, |name| {
                name.to_string_lossy() >= p.file_name().unwrap().to_string_lossy()
            })
        }) {
            // aggiungi la entry al marker (buffer)
            if !marker.add_file(&file_entry.name, file_entry.attr) {
                break; // buffer pieno, esci dal ciclo
            }
        }

        Ok(marker.bytes_written() as u32)
    

    }

    fn create(
        &self,
        path: &U16CStr,
        create_options: u32,
        granted_access: u32,
        file_attributes: u32,
        allocation_size: Option<&[c_void]>,
        create_flags: u64,
        reserved: Option<&[u8]>,
        write_through: bool,
        file_info: &mut OpenFileInfo,
    ) -> Result<Self::FileContext, FspError> {
        // Quando viene creato un file o directory:
        // -> se è dir: POST /mkdir/<path>
        // -> se è file: PUT /files/<path> con body vuoto
        // TODO: chiamare self.api_create_directory() o self.api_write_file()
        let path_str = self.path_from_u16(path);
        let is_dir = (create_options & winfsp::filesystem::CREATE_DIRECTORY) != 0;

        // Creazione fisica nel backend remoto directoryi se il bit era a 1 se no file
        if is_dir {
           
            self.rt.block_on(self.api.create_directory(&path_str))
                .map_err(|_| FspError::from_win32())?;
        } else {
            self.rt.block_on(self.api.write_file(&path_str, ""))
                .map_err(|_| FspError::from_win32())?;
        }

        file_info.basic.file_attributes = if is_dir {
            winfsp::filesystem::FILE_ATTRIBUTE_DIRECTORY
        } else {
            0
        };
        file_info.basic.file_size = 0;

        // Apri il file creato se è un file e se concessi permessi di scrittura
        if !is_dir && (granted_access & Self::FILE_WRITE_DATA != 0) {
            // Riuso la funzione open che crea il contesto corretto e TempWrite
            return self.open(path, create_options, granted_access, file_info);
        }

        // Per directory o file senza scrittura, ritorna contesto semplice
        let ino = self.alloc_ino(Path::new(&path_str));
        Ok(MyFileContext { ino, temp_write: None })
    }

}



pub fn mount_fs(mountpoint: &str, api: FileApi) -> anyhow::Result<()> {
    let fs = RemoteFs::new();
    let mut vparams = VolumeParams::default();
    vparams.sectors_per_allocation_unit(64); // Numero di settori per unità di allocazione
    vparams.sector_size(4096); // Dimensione di settore (4096 bytes)
    vparams.file_info_timeout(5); // Timeout per caching info file (in secondi);
    let mut host = FileSystemHost::new(vparams, fs)?;
    host.mount(mountpoint)?;
    host.start()?;

    Ok(())
}
