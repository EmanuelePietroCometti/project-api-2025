use std::ffi::c_void;
use widestring::U16CStr;

use winfsp::filesystem::{DirMarker, FileSecurity, FileSystemContext, OpenFileInfo, FileInfo};
use winfsp::host::{FileSystemHost, VolumeParams};

use winfsp::FspError;

pub struct MyFileContext;

#[derive(Clone)]
pub struct RemoteFs;
impl RemoteFs {
    pub fn new() -> Self {
        RemoteFs
    }
    pub fn delete(&self, path: &U16CStr) -> Result<(), FspError> {
        // DELETE /files/<path>
        Ok(())
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

    fn open(
        &self,
        _path: &U16CStr,
        _create_options: u32,
        _granted_access: u32,
        _file_info: &mut OpenFileInfo,
    ) -> Result<Self::FileContext, FspError> {
        // Operazione chiamata quando un file viene aperto.
        // Potrai verificare se esiste il file chiamando:
        // await self.api_read_file(path)
        // o semplicemente restituire un contesto vuoto.
        Ok(MyFileContext)
    }

    fn close(&self, _file_context: Self::FileContext) {
        // Chiusura file. Normalmente non serve fare nulla.
    }

    fn read(
        &self,
        _file_context: &Self::FileContext,
        _buffer: &mut [u8],
        _offset: u64,
    ) -> Result<u32, FspError> {
        // Quando un'applicazione legge un file locale:
        // -> effettua una GET /files/<path> al backend
        // -> copia i byte nel buffer locale
        // TODO: inserire codice asincrono per leggere i dati
        unimplemented!()
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
        // -> invia i byte al backend tramite PUT /files/<path>
        // TODO: inserire codice asincrono per scrittura remota
        unimplemented!()
    }

    fn read_directory(
        &self,
        _file_context: &Self::FileContext,
        _pattern: Option<&U16CStr>,
        mut _marker: DirMarker<'_>,
        _buffer: &mut [u8],
    ) -> Result<u32, FspError> {
        // Quando viene fatto "dir" o "ls":
        // -> chiama GET /list/<path> dal backend
        // -> per ogni entry restituita, aggiungi a `marker.add_file(name, attributes)`
        // TODO: chiamare self.api_list_directory() appena async disponibile
        unimplemented!()
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
        unimplemented!()
    }
}

pub fn mount_fs(mountpoint: &str) -> anyhow::Result<()> {
    let fs = RemoteFs::new();
    let vparams = VolumeParams::default();
    let mut host = FileSystemHost::new(vparams, fs)?;

    host.mount(mountpoint)?;
    host.start()?;

    Ok(())
}
