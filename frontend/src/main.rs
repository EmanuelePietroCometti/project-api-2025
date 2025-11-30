use frontend::{file_api::FileApi, mount_fs};
use std::{net::IpAddr, path::PathBuf};
use std::io::{self, Write};
fn main() -> anyhow::Result<()> {
    let mut ip_address = String::new();
    print!("Insert the backend IP address: ");
    io::stdout().flush()?;
    std::io::stdin().read_line(&mut ip_address)?;
    if ip_address.is_empty(){
        return Err(anyhow::anyhow!("IP address cannot be empty"));
    } else {
        let ip_trimmed = ip_address.trim();
        let _addr: IpAddr = ip_trimmed.parse().map_err(|_| anyhow::anyhow!("Invalid IP address format"))?;
        ip_address = ip_trimmed.to_string();
    } 
    let url = format!("http://{}:3001", ip_address);
    println!("Using backend URL: {}", url);
    let home_dir = dirs::home_dir().expect("Failed to get home directory");
    let mountpoint = PathBuf::from(home_dir).join("mnt").join("remote-fs");
    let mp = mountpoint.to_string_lossy().to_string();
    println!("Mounting filesystem at: {}", mp);
    let api = FileApi::new(&url);
    mount_fs(&mp, api, url)
}
