
pub mod directoryapi {
    use std::path;
    use serde::Deserialize;
    use serde_json;

    use reqwest::Client;
    

    //#[tokio::main]
    pub async fn mkdir(path: &str)-> Result<(),reqwest::Error>{
        let origin= "http://localhost:3001";
        let client= Client::new();

        let res= client
                .post(format!("{}/mkdir",origin))
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
    //#[tokio::main]
    //pub async fn ls(path:&str)->Result<Vec<DirectoryEntry>,reqwest::Error>{
    pub async fn ls(path:&str)->Result<Vec<DirectoryEntry>,Box<dyn std::error::Error>>{
        let origin= "http://localhost:3001";
        let client= Client::new();

       /*  let res= client
                .get(format!("{}/list/{}", origin,path))
                .send()
                .await?
                .json::<Vec<DirectoryEntry>>()
                .await?;*/

        let res = client
            .get(format!("{}/list/{}", origin, path))
            .send()
            .await?;

        let text = res.text().await?;
        println!("DEBUG risposta server: {}", text);

        let parsed: Vec<DirectoryEntry> = serde_json::from_str(&text)?;
        Ok(parsed)

       // Ok(res)

    }
}


