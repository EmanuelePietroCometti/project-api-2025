use anyhow::Result;
use frontend::{file_api::FileApi,mount_fs};
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

#[cfg(target_os = "windows")]
use windows_service::{
    service::ServiceControl,
    service_control_handler::{self, ServiceControlHandlerResult},
};

#[cfg(target_os = "windows")]
use winapi::um::wincon::GenerateConsoleCtrlEvent;

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

fn get_resolved_mountpoint() -> Result<String> {
    let home_dir = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Impossibile trovare la Home directory"))?;
    let mp = home_dir.join("mnt").join("remote-fs");
    
    if !mp.exists() {
        fs::create_dir_all(&mp)?;
    }
    
    Ok(mp.to_string_lossy().to_string())
}

fn is_mountpoint_busy(path: &str) -> bool {
    #[cfg(target_os = "linux")]
    {
        let output = std::process::Command::new("fuser")
            .arg("-m")
            .arg(path)
            .output();
        return match output {
            Ok(out) => out.status.success(),
            Err(_) => false,
        };
    }

    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("lsof")
            .arg("-wn")
            .arg("+d")
            .arg(path)
            .output();
        return match output {
            Ok(out) => out.status.success(),
            Err(_) => false,
        };
    }

    #[cfg(target_os = "windows")]
    {
        let script = format!(
            "if (Get-Process | Where-Object {{ $_.Path -like '*{path}*' -or (Get-WmiObject Win32_Process -Filter \"ProcessId=$($_.Id)\").ExecutablePath -like '*{path}*' }}) {{ exit 0 }} else {{ exit 1 }}",
            path = path.replace("\\", "\\\\")
        );
        let output = std::process::Command::new("powershell")
            .arg("-Command")
            .arg(&script)
            .output();
        return match output {
            Ok(out) => out.status.success(),
            Err(_) => false,
        };
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    false
}

fn main() -> Result<(), anyhow::Error> {
    let args: Vec<String> = env::args().collect();
    
    if args.contains(&"--stop".to_string()) {
        let mp = get_resolved_mountpoint()?;
        if is_mountpoint_busy(&mp) {
            eprintln!("Errore: Il filesystem in {} è occupato.", mp);
            return Ok(());
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return stop_deamon();
        #[cfg(target_os = "windows")]
        return stop_service();
    }

    let mut ip_input = String::new();
    print!("Inserisci l'indirizzo IP del backend: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut ip_input)?;
    let ip = ip_input.trim().to_string();

    ip.parse::<IpAddr>().map_err(|_| anyhow::anyhow!("Formato IP non valido: {}", ip))?;

    let mp = get_resolved_mountpoint()?;

    if args.contains(&"--deamon".to_string()) {
        println!("Avvio del filesystem in background su {}...", mp);
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return run_as_deamon(&ip, &mp);
        
        #[cfg(target_os = "windows")]
        return run_as_service(&ip, &mp);
    }

    start_filesystem(&ip, &mp)
}

fn start_filesystem(ip: &str, mp: &str) -> anyhow::Result<()> {
    let url = format!("http://{}:3001", ip);
    let api = FileApi::new(&url);
    let rt = tokio::runtime::Runtime::new()?;
    
    rt.block_on(FileApi::health(ip))?;
    
    if cfg!(debug_assertions) {
        println!("[START] Connesso al backend. Mountpoint: {}", mp);
    }

    mount_fs(mp, api, url)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn run_as_deamon(ip: &str, mp: &str) -> anyhow::Result<()> {
    let daemon = Daemonize::new()
        .pid_file(pid_file())
        .working_directory(env::current_dir().unwrap_or_else(|_| PathBuf::from("/")))
        .stdout(fs::File::create("/tmp/remote_fs.out")?)
        .stderr(fs::File::create("/tmp/remote_fs.err")?);

    daemon.start().map_err(|e| anyhow::anyhow!("Errore demone: {}", e))?;
    
    let _ = write_pid();
    let res = start_filesystem(ip, mp);
    remove_pid();
    res
}

#[cfg(target_os = "windows")]
fn run_as_service(ip: &str, mp: &str) -> anyhow::Result<()> {
    let handler = move |event| -> ServiceControlHandlerResult {
        match event {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                unsafe { GenerateConsoleCtrlEvent(winapi::um::wincon::CTRL_BREAK_EVENT, 0); }
                ServiceControlHandlerResult::NoError
            }
            _ => ServiceControlHandlerResult::NoError,
        }
    };

    let _status_handle = service_control_handler::register("RemoteFsService", handler)?;
    
    let _ = write_pid();
    let res = start_filesystem(ip, mp);
    remove_pid();
    res
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn stop_deamon() -> anyhow::Result<()> {
    let pid_str =
        fs::read_to_string(pid_file()).map_err(|_| anyhow::anyhow!("PID file not found"))?;

    let pid: i32 = pid_str.trim().parse()?;

    match kill(Pid::from_raw(pid), Signal::SIGTERM) {
        Ok(_) => {
            println!("SIGTERM sent to {}", pid);
            Ok(())
        }
        Err(nix::errno::Errno::ESRCH) => {
            remove_pid();
            println!("Process already stopped");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(target_os = "windows")]
fn stop_service() -> anyhow::Result<()> {
    let pid_str =
        fs::read_to_string(pid_file()).map_err(|_| anyhow::anyhow!("PID file not found"))?;

    let pid: i32 = pid_str.trim().parse()?;

    unsafe {
        GenerateConsoleCtrlEvent(winapi::um::wincon::CTRL_BREAK_EVENT, pid);
    }

    println!(
        "Sent CTRL_BREAK_EVENT to PID {}\nRemote filesystem unmounted!",
        pid
    );
    Ok(())
}
