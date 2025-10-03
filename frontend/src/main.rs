use frontend::mount_fs;

fn main() {
    let mountpoint="mnt/remote-fs";
    mount_fs(mountpoint);
}
