pub mod file_api;

#[cfg(target_os = "linux")]
mod fuse_linux;

#[cfg(target_os = "linux")]
pub use fuse_linux::{mount_fs,is_mountpoint_busy};

#[cfg(target_os = "macos")]
mod fuse_mac;

#[cfg(target_os = "macos")]
pub use fuse_mac::{mount_fs, is_mountpoint_busy};
#[cfg(target_os = "windows")]
mod fuse_windows;

#[cfg(target_os = "windows")]
pub use fuse_windows::{mount_fs, is_mountpoint_busy};

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
