mod fileapi;
use fileapi::FileApi;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let api = FileApi::new("http://localhost:3001");


   api.ls("testdir").await?;



    

    


    Ok(())
}
