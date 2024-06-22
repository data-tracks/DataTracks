use std::path::PathBuf;
use std::process::Command;

fn main() {
    // Set the current directory to the frontend directory
    let ui_dir = PathBuf::from("ui");

    // Run `npm install` to install dependencies
    /*let npm_install_status = Command::new("pnpm")
        .arg("install")
        .current_dir(&frontend_dir)
        .status()
        .expect("Failed to run pnpm install");

    assert!(npm_install_status.success(), "pnpm install failed");*/

    // Run `npm run build` to build the Vue.js application
    let npm_build_status = Command::new("pnpm")
        .arg("build")
        .current_dir(&ui_dir)
        .status()
        .expect("Failed to run pnpm run build");

    let success = npm_build_status.success();
    if !success {
        println!("Could not build, using old")
    }
}