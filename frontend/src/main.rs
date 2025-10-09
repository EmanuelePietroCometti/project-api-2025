mod fileApi;
use fileApi::FileApi;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let api = FileApi::new("http://localhost:3001");

   api.delete("testdir3").await?;


    /*   println!("1️⃣ mkdir...");
    api.mkdir("testdir3").await?;

    //println!("2️⃣ write_file...");
    api.write_file("testdir3/ema.txt", "./ema.txt").await?;

   // println!("3️⃣ ls...");
    api.ls("testdir3").await?;

    //println!("✅ Done.");*/



    

    


    Ok(())
}
