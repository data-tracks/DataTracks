use std::fs;
use std::process::Command;

fn main() {
    setup_webui();
}

fn setup_webui() {
    let ui_path = "../dashboard/src"; // Path to your .fbs file

    let dist_path = "../dashboard/dist/dashboard/browser/";
    fs::create_dir_all(dist_path).expect("Failed to create dist directory");

    // rerun this script if the schema changes
    println!("cargo:rerun-if-changed={}", ui_path);

    // call pnpm based on the OS
    let mut cmd = if cfg!(target_os = "windows") {
        let mut c = Command::new("cmd");
        c.args(["/C", "pnpm"]);
        c
    } else {
        Command::new("pnpm")
    };

    cmd
        .args([
            "install",
        ])
        .current_dir("../dashboard")
        .status()
        .expect("Failed to install dependencies. Is pnpm it installed?");


    cmd
        .args(["run", "build"])
        .current_dir("../dashboard")
        .status()
        .expect("Failed to build webui.");

}