pub mod fs;

#[cfg(test)]
mod tests {
    use super::fs::directoryapi;

    #[tokio::test]
    async fn test_mkdir_and_ls() {
        let path = "test_rust_api";
        directoryapi::mkdir(path).await.unwrap();
        println!("directory creata con successo");

        let entries = directoryapi::ls(path).await.unwrap();
        println!("Entries: {:?}", entries);
    }
}
