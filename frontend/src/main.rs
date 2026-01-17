use anyhow::Result;
use frontend::{file_api::FileApi, mount_fs};
use std::{
    env, fs,
    io::{self, Write},
    net::IpAddr,
    path::PathBuf,
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use daemonize::Daemonize;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use nix::{
    sys::signal::{kill, Signal},
    unistd::Pid,
};

fn pid_file() -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push("remote-fs");
    let _ = std::fs::create_dir_all(&dir);
    dir.push("pid");
    dir
}

fn write_pid() -> anyhow::Result<()> {
    let pid = std::process::id();
    std::fs::write(pid_file(), pid.to_string())?;
    Ok(())
}

fn remove_pid() {
    let _ = std::fs::remove_file(pid_file());
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn get_resolved_mountpoint() -> Result<String> {
    let home_dir = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Impossibile trovare la Home directory"))?;
    let mp = home_dir.join("mnt").join("remote-fs");
    if !mp.exists() {
        fs::create_dir_all(&mp)?;
    }
    Ok(mp.to_string_lossy().to_string())
}

#[cfg(target_os = "windows")]
fn get_resolved_mountpoint() -> Result<String> {
    let home_dir = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Impossibile trovare la Home directory"))?;
    let mnt_dir = home_dir.join("mnt");
    let mp = mnt_dir.join("remote-fs");

    if !mnt_dir.exists() {
        fs::create_dir_all(&mnt_dir)?;
    }

    if mp.exists() {
        let _ = fs::remove_dir(&mp);
    }
    
    Ok(mp.to_string_lossy().to_string())
}

fn main() -> Result<(), anyhow::Error> {
    let args: Vec<String> = env::args().collect();
    
    if args.contains(&"--stop".to_string()) {
        println!("Tentativo di arresto del filesystem remoto...");
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return stop_daemon();
        #[cfg(target_os = "windows")]
        return stop_windows_process();
    }

    let ip = if args.len() > 1 && args[1].parse::<IpAddr>().is_ok() {
        args[1].clone()
    } else if args.contains(&"--deamon".to_string()) {
        return Err(anyhow::anyhow!("Errore: IP mancante per l'avvio in background.\nUso: cargo run -- <IP> --deamon"));
    } else {
        let mut ip_input = String::new();
        print!("Inserisci l'indirizzo IP del backend: ");
        io::stdout().flush()?;
        io::stdin().read_line(&mut ip_input)?;
        ip_input.trim().to_string()
    };

    ip.parse::<IpAddr>().map_err(|_| anyhow::anyhow!("Formato IP non valido: {}", ip))?;
    let mp = get_resolved_mountpoint()?;

    if args.contains(&"--deamon".to_string()) {
        println!("Avvio del filesystem in background su {}...", mp);
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return run_as_daemon_unix(&ip, &mp);
        
        #[cfg(target_os = "windows")]
        return run_as_detached_windows(&ip, &mp);
    }

    start_filesystem(&ip, &mp)
}

fn start_filesystem(ip: &str, mp: &str) -> anyhow::Result<()> {
    write_pid()?;
    
    let url = format!("http://{}:3001", ip);
    let api = FileApi::new(&url);
    let rt = tokio::runtime::Runtime::new()?;
    
    rt.block_on(FileApi::health(ip))?;
    
    if cfg!(debug_assertions) {
        println!("[START] Connesso al backend. Mountpoint: {}", mp);
    }

    let res = mount_fs(mp, api, url);
    
    remove_pid();
    res
}


#[cfg(any(target_os = "linux", target_os = "macos"))]
fn run_as_daemon_unix(ip: &str, mp: &str) -> anyhow::Result<()> {
    let daemon = Daemonize::new()
        .pid_file(pid_file())
        .working_directory(env::current_dir().unwrap_or_else(|_| PathBuf::from("/")))
        .stdout(fs::File::create("/tmp/remote_fs.out")?)
        .stderr(fs::File::create("/tmp/remote_fs.err")?);

    daemon.start().map_err(|e| anyhow::anyhow!("Errore demone: {}", e))?;
    
    start_filesystem(ip, mp)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn stop_daemon() -> anyhow::Result<()> {
    let pid_str = fs::read_to_string(pid_file()).map_err(|_| anyhow::anyhow!("File PID non trovato. Il filesystem è attivo?"))?;
    let pid: i32 = pid_str.trim().parse()?;

    match kill(Pid::from_raw(pid), Signal::SIGTERM) {
        Ok(_) => {
            println!("Segnale di arresto inviato al processo {}", pid);
            remove_pid();
            Ok(())
        }
        Err(nix::errno::Errno::ESRCH) => {
            remove_pid();
            println!("Processo già terminato.");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(target_os = "windows")]
fn run_as_detached_windows(ip: &str, _mp: &str) -> anyhow::Result<()> {
    use std::os::windows::process::CommandExt;
    
    let child = std::process::Command::new(std::env::current_exe()?)
        .arg(ip)
        .creation_flags(0x00000008) 
        .spawn();

    match child {
        Ok(_) => {
            println!("[INFO] Processo avviato correttamente in background.");
            std::process::exit(0);
        }
        Err(e) => Err(anyhow::anyhow!("Errore durante l'avvio del processo: {}", e)),
    }
}

#[cfg(target_os = "windows")]
fn stop_windows_process() -> anyhow::Result<()> {
    let pid_file_path = pid_file();
    let pid_str = fs::read_to_string(&pid_file_path)
        .map_err(|_| anyhow::anyhow!("File PID non trovato. Il filesystem è attivo?"))?;

    let pid: u32 = pid_str.trim().parse()?;

    let output = std::process::Command::new("taskkill")
        .arg("/F")
        .arg("/PID")
        .arg(pid.to_string())
        .output();

    match output {
        Ok(out) if out.status.success() => {
            println!("Processo {} terminato con successo.", pid);
            let _ = fs::remove_file(pid_file_path);
            Ok(())
        }
        _ => {
            let _ = fs::remove_file(pid_file_path);
            Err(anyhow::anyhow!("Impossibile terminare il processo {}. Potrebbe essere già stato chiuso.", pid))
        }
    }
}