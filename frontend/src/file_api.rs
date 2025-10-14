use anyhow::{Result, anyhow};
use reqwest::{Body, Client};
use serde::Deserialize;
use tokio::fs;
use tokio::io::AsyncReadExt;
use std::time::SystemTime;
#[derive(Clone)]
pub struct FileApi {
    base_url: String,
    client: Client,
}

#[derive(Deserialize, Debug, Clone)]
pub struct DirectoryEntry {
    //struct in cui mettiamo i valori da stampare nel ls
    pub name: String,
    pub size: i64,
    pub mtime: i64,
    pub permissions: String,
    pub is_dir: i64,
    pub version: i64,
}

impl FileApi {
    pub fn new(base_url: &str) -> Self {
        FileApi {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: Client::new(),
        }
    }

    pub async fn chmod(&self, rel_path: &str, mode: u32) -> anyhow::Result<()> {
        let url = format!("{}/files/chmod", self.base_url);
        let perm = format!("{:o}", mode & 0o777);
        let resp = self
            .client
            .patch(&url)
            .query(&[("relPath", rel_path), ("perm", perm.as_str())])
            .send()
            .await?;
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!(
                "chmod failed: {} - {}",
                status,
                text
            ))
        }
    }

    pub async fn truncate(&self, rel_path: &str, size: u64) -> anyhow::Result<()> {
        let url = format!("{}/files/truncate", self.base_url);
        let resp = self
            .client
            .patch(&url)
            .query(&[("relPath", rel_path), ("size", &size.to_string())])
            .send()
            .await?;
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!(
                "truncate failed: {} - {}",
                status,
                text
            ))
        }
    }

    pub async fn utimes(
        &self,
        rel_path: &str,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> anyhow::Result<()> {
        let url = format!("{}/files/utimes", self.base_url);
        // Converti SystemTime in secondi Unix
        let ts = |t: SystemTime| {
            t.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_string()
        };
        let mut q: Vec<(&str, String)> = vec![("relPath", rel_path.to_string())];
        if let Some(a) = atime {
            q.push(("atime", ts(a)));
        }
        if let Some(m) = mtime {
            q.push(("mtime", ts(m)));
        }
        let resp = self.client.patch(&url).query(&q).send().await?;
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!(
                "utimes failed: {} - {}",
                status,
                text
            ))
        }
    }

    /// GET /files?relPath=...
    pub async fn read_file(&self, rel_path: &str) -> Result<Vec<u8>> {
        let url = format!("{}/files", self.base_url);

        let resp = self
            .client
            .get(&url)
            .query(&[("relPath", rel_path)])
            .send()
            .await?;

        let status = resp.status();

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("read_file failed: {} - {}", status, text));
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

        let resp = self
            .client
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
            Err(anyhow!("write_file failed: {} - {}", status, text))
        }
    }

    /// DELETE /files?relPath=...
    pub async fn delete(&self, rel_path: &str) -> Result<()> {
        let url = format!("{}/files", self.base_url);

        let resp = self
            .client
            .delete(&url)
            .query(&[("relPath", rel_path)])
            .send()
            .await?;

        let status = resp.status();
        if resp.status().is_success() {
            Ok(())
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(anyhow!("delete failed: {} - {}", status, text))
        }
    }

    pub async fn mkdir(&self, path: &str) -> Result<()> {
        let resp = self
            .client
            .post(format!("{}/mkdir", self.base_url))
            .query(&[("relPath", path)])
            .send()
            .await?;

        let status = resp.status();
        if resp.status().is_success() {
            Ok(())
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(anyhow!("mkdir failed: {} - {}", status, text))
        }
    }

    pub async fn ls(&self, path: &str) -> Result<Vec<DirectoryEntry>> {
        let resp = self
            .client
            .get(format!("{}/list", self.base_url))
            .query(&[("relPath", path)])
            .send()
            .await?;

        let status = resp.status();
        if resp.status().is_success() {
            let v = resp.json::<Vec<DirectoryEntry>>().await?;
            // println!("Response text: {:?}", v);
            Ok(v)
        } else {
            let text = resp.text().await.unwrap_or_default();
            // println!("Error response text: {}", text);
            Err(anyhow!("ls failed: {} - {}", status, text))
        }
    }
}
