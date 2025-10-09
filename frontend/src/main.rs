use frontend::{file_api::FileApi, mount_fs};

fn main() -> anyhow::Result<()> {
    let mountpoint = if cfg!(target_os = "windows") {
        r"Z:"
    } else {
        "/mnt/remote-fs" 
    };
    let api = FileApi::new("http://localhost:3000");
    crate::mount_fs(mountpoint, api)
}
