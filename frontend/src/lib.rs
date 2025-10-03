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