mod fileapi;
use fileapi::FileApi;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let api = FileApi::new("http://localhost:3001");


     // Scrive un file remoto
    api.write_file("testdir/file.txt", "./local.txt").await?;
    println!("✅ File scritto correttamente");
    
    // Legge lo stesso file
    let data = api.read_file("testdir/file.txt").await?;
    println!("📖 File remoto: {} bytes", data.len());

    // Cancella il file
    api.delete("testdir/file.txt").await?;
    println!("🗑️ File eliminato");

    


    Ok(())
}
