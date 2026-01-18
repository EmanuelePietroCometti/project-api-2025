pub mod file_api;

use std::path::PathBuf;

pub fn status_file() -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push("remote-fs");
    let _ = std::fs::create_dir_all(&dir);
    dir.push("status");
    dir
}

pub fn write_status(msg: &str) {
    let _ = std::fs::write(status_file(), msg);
}

pub fn clear_status() {
    let _ = std::fs::remove_file(status_file());
}

#[cfg(target_os = "linux")]
mod fuse_linux;

#[cfg(target_os = "linux")]
pub use fuse_linux::mount_fs;

#[cfg(target_os = "macos")]
mod fuse_mac;

#[cfg(target_os = "macos")]
pub use fuse_mac::mount_fs;
#[cfg(target_os = "windows")]
mod fuse_windows;

#[cfg(target_os = "windows")]
pub use fuse_windows::mount_fs;

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "windows"
)))]
pub fn mount_fs(
    _mountpoint: &str,
    _api: file_api::FileApi,
    _url: String
) -> anyhow::Result<()> {
    Err(anyhow::anyhow!(
        "mount_fs is only supported on Linux, macOS or Windows"
    ))
}
