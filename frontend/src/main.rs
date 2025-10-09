use frontend::mount_fs;

fn main() -> anyhow::Result<()> {
    let mountpoint = if cfg!(target_os = "windows") {
        r"Z:"
    } else {
        "/mnt/remote-fs" 
    };
    crate::mount_fs(mountpoint)
}
