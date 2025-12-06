use frontend::{file_api::FileApi, mount_fs};
use std::{net::IpAddr};
use std::env;


fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    println!("{args:?}");

    let ip_address = args.get(1)
        .ok_or_else(|| anyhow::anyhow!("IP argument missing"))?
        .trim()
        .to_string();

    ip_address.parse::<IpAddr>()
        .map_err(|_| anyhow::anyhow!("Invalid IP address format"))?;

    let url = format!("http://{}:3001", ip_address);

    let home_dir = dirs::home_dir().expect("Failed to get home directory");
    let mountpoint = home_dir.join("mnt").join("remote-fs");
    let mp = mountpoint.to_string_lossy().to_string();

    let api = FileApi::new(&url);
    mount_fs(&mp, api, url)
}
