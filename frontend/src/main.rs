use frontend::{file_api::FileApi, mount_fs};

fn main() -> anyhow::Result<()> {
    let mountpoint = if cfg!(target_os = "windows") {
        r"Z:"
    } else {
        "/home/emanuele-pietro-cometti/mnt/remote-fs" 
    };
    let mp=mountpoint.to_string();
    let api = FileApi::new("http://localhost:3001");
    mount_fs(&mp, api)
}
