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
    std::fs::create_dir_all(&dir).ok();
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

fn main() -> Result<(), anyhow::Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 && !args[1].starts_with("--") {
        let ip = args[1].trim().to_string();
        return start_filesystem(&ip);
    }

    if args.contains(&"--stop".to_string()) {
        println!("Unmounting remote filsystem...");
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return stop_deamon();
        #[cfg(target_os = "windows")]
        return stop_service();
    }

    let mut ip_address = String::new();
    print!("Insert the backend IP address: ");
    io::stdout().flush()?;
    std::io::stdin().read_line(&mut ip_address)?;
    let ip = ip_address.trim().to_string();

    ip.parse::<IpAddr>()
        .map_err(|_| anyhow::anyhow!("Invalid IP format"))?;

    if args.contains(&"--deamon".to_string()) {
        println!("Mounting remote filsystem...");
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return run_as_deamon(&ip);
        #[cfg(target_os = "windows")]
        return run_as_service(&ip);
    }

    start_filesystem(&ip)
}

fn start_filesystem(ip: &str) -> anyhow::Result<()> {
    let url = format!("http://{}:3001", ip);
    let home_dir = dirs::home_dir().expect("Failed to get home directory");
    let mp = PathBuf::from(home_dir)
        .join("mnt")
        .join("remote-fs")
        .to_string_lossy()
        .to_string();
    let api = FileApi::new(&url);
    mount_fs(&mp, api, url)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn run_as_deamon(ip: &str) -> anyhow::Result<()> {
    let daemon = Daemonize::new()
        .pid_file(pid_file())
        .working_directory(env::current_dir().unwrap_or_else(|_| PathBuf::from("/")))
        .stdout(fs::File::create("/tmp/remote_fs.out")?)
        .stderr(fs::File::create("/tmp/remote_fs.err")?);
    daemon
        .start()
        .map_err(|e| anyhow::anyhow!("Daemon failed: {}", e))?;
    write_pid()?;
    let res = start_filesystem(ip);
    remove_pid();
    res
}

#[cfg(target_os = "windows")]
fn run_as_service(ip: &str) -> anyhow::Result<()> {
    let handler = move |event| -> ServiceControlHandlerResult {
        match event {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                unsafe {
                    GenerateConsoleCtrlEvent(winapi::um::wincon::CTRL_BREAK_EVENT, 0);
                }
                ServiceControlHandlerResult::NoError
            }
            _ => ServiceControlHandlerResult::NoError,
        }
    };

    service_control_handler::register("RemoteFsService", handler)?;

    write_pid()?;
    let res = start_filesystem(ip);
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
