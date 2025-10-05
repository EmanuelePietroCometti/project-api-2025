#[cfg(target_os = "linux")]
mod fuse_linux;
#[cfg(target_os = "linux")]
pub use fuse_linux::mount_fs;

#[cfg(target_os = "macos")]
mod fuse_mac;
#[cfg(target_os = "macos")]
pub use fuse_mac::mount_fs;

#[cfg(all(target_os = "windows", feature = "windows"))]
mod fuse_windows;
#[cfg(all(target_os = "windows", feature = "windows"))]
pub use fuse_windows::mount_fs;

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    all(target_os = "windows", feature = "windows")
)))]
pub fn mount_fs(_mountpoint: &str) -> anyhow::Result<()> {
    Err(anyhow::anyhow!(
        "mount_fs is only available on supported OS targets (Linux/macOS/Windows with --features windows)"
    ))
}
