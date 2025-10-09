use anyhow::{anyhow, Result};
use reqwest::{Client, Body};
use tokio::fs;
use tokio::io::AsyncReadExt;
use std::path;
use serde::Deserialize;
use serde_json;

#[derive(Clone)]
pub struct FileApi {
    base_url: String,
    client: Client,
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

     pub async fn mkdir(&self, path: &str)-> Result<(),reqwest::Error>{

        let res= self.client
                .post(format!("{}/mkdir",self.base_url))
                .query(&[("relPath",path)])
                .send()
                .await?;

        Ok(())    
    }


    #[derive(Deserialize, Debug)]
    pub struct DirectoryEntry{//struct in cui mettiamo i valori da stampare nel ls
        name:String,
        size: u64,
        mtime: u64,//vediamo se mettere date
        permission: String,
    }


    pub async fn ls(&self, path:&str)->Result<Vec<DirectoryEntry>,Box<dyn std::error::Error>>{

       /*  let res= client
                .get(format!("{}/list/{}", origin,path))
                .send()
                .await?
                .json::<Vec<DirectoryEntry>>()
                .await?;*/

        let res = self.client
            .get(format!("{}/list/{}", self.base_url, path))
            .send()
            .await?;

        let text = res.text().await?;
        println!("DEBUG risposta server: {}", text);

        let parsed: Vec<DirectoryEntry> = serde_json::from_str(&text)?;
        Ok(parsed)

       // Ok(res)

    }


}
