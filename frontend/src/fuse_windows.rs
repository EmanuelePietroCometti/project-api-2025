use std::ffi::OsStr;
use std::sync::Arc;
use winfsp::{FileSystem, FileSystemOptions, FileSystemServer};

// Add mod with API calls

pub struct RemoteFs;

impl FileSystem for RemoteFs {
    fn readdir(
        &self,
        path: &OsStr,
    ) -> winfsp::Result<Vec<winfsp::DirEntry>> {
        // API call to list directory contents
        unimplemented!()
    }
    fn read(
        &self,
        path: &OsStr,
        offset: u64,
        data: &mut [u8],
    ) -> winfsp::Result<u32> {
        // API call to read file content
        unimplemented!()
    }

    fn write(
        &self,
        path: &OsStr,
        offset: u64,
        data: &[u8],
    ) -> winfsp::Result<u32> {
        // API call to write file content
        unimplemented!()
    }

    fn mkdir(
        &self,
        path: &OsStr,
    ) -> winfsp::Result<()> {
        // API call to create a directory
        unimplemented!()
    }

    fn unlink(
        &self,
        path: &OsStr,
    ) -> winfsp::Result<()> {
        // API call to delete a file
        unimplemented!()
    }
}

pub fn mount_fs(mountpoint: &str) {
    let fs = Arc::new(RemoteFs);
    let options = FileSystemOptions::new()
        .mount_point(mountpoint) 
        .file_system_name("RemoteFS")
        .with_debug(true)
        .build()?;
    let server = FileSystemServer::new(fs, mountpoint, &options).unwrap();
    server.start()?;
    Ok(())
}   
