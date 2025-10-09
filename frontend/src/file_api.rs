use anyhow::{anyhow, Result};
use reqwest::{Client, Body};
use tokio::fs;
use tokio::io::AsyncReadExt;
use serde::Deserialize;

#[derive(Clone)]
pub struct FileApi {
    base_url: String,
    client: Client,
}

 #[derive(Deserialize, Debug)]
    pub struct DirectoryEntry{//struct in cui mettiamo i valori da stampare nel ls
        pub name:String,
        pub size: i64,
        pub mtime: i64,
        pub permissions: String,
    }

impl FileApi {
    pub fn new(base_url: &str) -> Self {
        FileApi {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: Client::new(),
        }
    }

    /// GET /files?relPath=...
    pub async fn read_file(&self, rel_path: &str) -> Result<Vec<u8>> {
        let url = format!("{}/files", self.base_url);

        let resp = self.client
            .get(&url)
            .query(&[("relPath", rel_path)])
            .send()
            .await?;

        let status = resp.status();
            
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "read_file failed: {} - {}",
                status,
                text
            ));
        }

        let bytes = resp.bytes().await?;
        Ok(bytes.to_vec())
    }

    /// PUT /files?relPath=...
    pub async fn write_file(&self, rel_path: &str, local_path: &str) -> Result<()> {
        let url = format!("{}/files", self.base_url);

        let mut file = fs::File::open(local_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let resp = self.client
            .put(&url)
            .query(&[("relPath", rel_path)])
            .body(Body::from(buffer))
            .send()
            .await?;

        let status = resp.status();
        if resp.status().is_success() {
            Ok(())
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(anyhow!(
                "write_file failed: {} - {}",
                status,
                text
            ))
        }
    }

    /// DELETE /files?relPath=...
    pub async fn delete(&self, rel_path: &str) -> Result<()> {
        let url = format!("{}/files", self.base_url);

        let resp = self.client
            .delete(&url)
            .query(&[("relPath", rel_path)])
            .send()
            .await?;

        let status = resp.status();
        if resp.status().is_success() {
            Ok(())
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(anyhow!(
                "delete failed: {} - {}",
                status,
                text
            ))
        }
    }

     pub async fn mkdir(&self, path: &str)-> Result<()>{

        let resp= self.client
                .post(format!("{}/mkdir",self.base_url))
                .query(&[("relPath",path)])
                .send()
                .await?;

        let status = resp.status();
        if resp.status().is_success() {
            Ok(())
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(anyhow!(
                "mkdir failed: {} - {}",
                status,
                text
            ))
        }    
    }



    pub async fn ls(&self, path:&str)->Result<Vec<DirectoryEntry>>{


         let resp= self.client
                .get(format!("{}/list/{}", self.base_url, path))
                .send()
                .await?;
                
     
        

       
        let status = resp.status();
        if resp.status().is_success() {
            let v= resp.json::<Vec<DirectoryEntry>>()
                .await?;

            println!("Response text: {:?}", v);
            Ok(v)
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(anyhow!(
                "ls failed: {} - {}",
                status,
                text
            ))
        }    

    }


}
