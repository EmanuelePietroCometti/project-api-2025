pub mod directoryapi {
    use reqwest::Client;
    use serde::Deserialize;
    use serde_json;

    //#[tokio::main]
    pub async fn mkdir(path: &str) -> Result<(), reqwest::Error> {
        let origin = "http://localhost:3001";
        let client = Client::new();

        let _res = client
            .post(format!("{}/mkdir", origin))
            .query(&[("relPath", path)])
            .send()
            .await?;

        Ok(())
    }

    #[derive(Deserialize, Debug)]
    pub struct DirectoryEntry {
        //struct in cui mettiamo i valori da stampare nel ls
        _name: String,
        _size: u64,
        _mtime: u64, //vediamo se mettere date
        _permission: String,
    }
    //#[tokio::main]
    //pub async fn ls(path:&str)->Result<Vec<DirectoryEntry>,reqwest::Error>{
    pub async fn ls(path: &str) -> Result<Vec<DirectoryEntry>, Box<dyn std::error::Error>> {
        let origin = "http://localhost:3001";
        let client = Client::new();

        /*  let res= client
        .get(format!("{}/list/{}", origin,path))
        .send()
        .await?
        .json::<Vec<DirectoryEntry>>()
        .await?;*/
        println!("Verifica di path: {}", path);
        let res = client
            .post(format!("{}/list", origin))
            .query(&[("relPath", path)])
            .send()
            .await?;

        let text = res.text().await?;
        println!("DEBUG risposta server: {}", text);

        let parsed: Vec<DirectoryEntry> = serde_json::from_str(&text)?;
        Ok(parsed)

        // Ok(res)
    }
}
