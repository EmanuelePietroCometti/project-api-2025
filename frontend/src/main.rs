use frontend::{file_api::FileApi, mount_fs};
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let home_dir = dirs::home_dir().expect("Failed to get home directory");
    let mountpoint = PathBuf::from(home_dir).join("mnt").join("remote-fs");
    let mp = mountpoint.to_string_lossy().to_string();
    println!("Mounting filesystem at: {}", mp);
    let api = FileApi::new("http://localhost:3001");
    mount_fs(&mp, api)
}
