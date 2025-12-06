use std::env;
use std::io::{self, Write};

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut ip_address = String::new();
    print!("Insert the backend IP address: ");
    let _ = io::stdout().flush();
    let _ = io::stdin().read_line(&mut ip_address);
    if args.get(1).map(|s| s == "deamon").unwrap_or(false){
        let ok = std::process::Command::new("pm2")
            .arg("start")
            .arg("./target/release/frontend")
            .arg("--name")
            .arg("client")
            .arg("--")
            .arg(&ip_address)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !ok {
            eprintln!("Error: invalid input");
        }
    } else {
        let ok = std::process::Command::new("./target/release/frontend")
            .arg("--")
            .arg(&ip_address)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !ok {
            eprintln!("Error: invalid input");
        }
    }
}
