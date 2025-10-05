use fuser016::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request, ReplyWrite, ReplyEmpty
};
use std::ffi::OsStr;
use std::time::{Duration, SystemTime};

pub struct RemoteFs;

impl Filesystem for RemoteFs {
    fn lookup(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEntry,
    ) {
        // API call to find file by name
        unimplemented!()
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: ReplyDirectory,
    ) {
        // API call to list directory contents
        unimplemented!()
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        // API call to read file content
        unimplemented!()
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        reply: fuser016::ReplyWrite,
    ) {
        unimplemented!()
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        reply: ReplyEntry,
    ) {
        // API call to create directory
        unimplemented!()
    }

    fn unlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEmpty,
    ) {
        // API call to delete file or directory
        unimplemented!()
    }
}

pub fn mount_fs(mountpoint: &str)-> anyhow::Result<()> {
    let fs = RemoteFs;
    let options = &[
        MountOption::AutoUnmount, 
        MountOption::AllowOther
    ];
    fuser015::mount2(fs, mountpoint, options)?;
    Ok(())
}
