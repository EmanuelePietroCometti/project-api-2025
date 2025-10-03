
pub mod directoryapi {
    use std::path;
    use serde::Deserialize;

    use reqwest::Client;
    

    #[tokio::main]
    async fn mkdir(path: &str)-> Result<(),reqwest::Error>{
        let origin= "http://localhost:5173/";
        let client= Client::new();

        let res= client
                .post(format!("{}/mkdir",origin))
                .query(&[("path",path)])
                .send()
                .await?;

        Ok(())    
    }

    #[derive(Deserialize, Debug)]
    struct DirectoryEntry{//struct in cui mettiamo i valori da stampare nel ls
        name:String,
        size: u64,
        mtime: u64,//vediamo se mettere date
        permission: String,
    }
    #[tokio::main]
    async fn ls(path:&str)->Result<Vec<DirectoryEntry>,reqwest::Error>{

        let origin= "http://localhost:5173/";
        let client= Client::new();

        let res= client
                .get(format!("{}/list/{}", origin,path))
                .send()
                .await?
                .json::<Vec<DirectoryEntry>>()
                .await?;

        Ok(res)

    }
}
