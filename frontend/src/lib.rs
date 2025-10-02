pub mod fs;  // qui vive la logica

use fs::MyFS;
use polyfuse::Session;
use anyhow::Result;
use tokio::signal;

pub async fn run(mountpoint: &str) -> Result<()> {
    let mut session = Session::mount(MyFS, mountpoint, &[]).await?;
    tokio::select! {
        res = session.run() => res?,
        _ = signal::ctrl_c() => {
            println!("Unmounting...");
        }
    }
    Ok(())
}

