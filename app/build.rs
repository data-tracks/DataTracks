use fs_extra::dir::{CopyOptions, copy};
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};

const UI_TARGET: &str = "../target/ui";

fn main() {
    let dir = Path::new(UI_TARGET);
    match fs::create_dir_all(dir) {
        Ok(_) => {}
        Err(err) => println!("error: {err}"),
    }

    // if ui folder is empty, try to rerun
    if fs::read_dir(UI_TARGET).unwrap().next().is_none() {
        println!("cargo:warn=Trying to rebuild UI...");

        build_ui();
    }
}

fn build_ui() {
    // --- 1. Configure Cargo Rerun Conditions ---
    // Tell Cargo to re-run this build script if any of these files/directories change.
    // This helps ensure the Angular UI is rebuilt when its configuration or dependencies change.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=ui/package.json");
    println!("cargo:rerun-if-changed=ui/angular.json");
    // You might want to add more specific directories if your Angular build is sensitive
    // to changes in certain source files, e.g., println!("cargo:rerun-if-changed=ui/angular-app/src");

    // --- 4. Check for External Dependencies (Node.js, npm, Angular CLI) ---
    println!("cargo:info=Checking for Node.js, pnpm, and Angular CLI...");
    if check_command("node", "Node.js is not installed or not in PATH. Please install Node.js (which includes pnpm) to build the Angular UI.").is_err() {
        println!("Build.rs was not able to build UI.");
        return;
    }
    if check_command("pnpm", "pnpm is not installed or not in PATH. Please install Node.js (which includes pnpm) to build the Angular UI.").is_err() {
        println!("Build.rs was not able to build UI.");
        return;
    }

    // --- 5. Install Angular Dependencies ---
    println!("cargo:info=Installing Angular UI dependencies (npm install)...");
    //println!("dir {:?}", angular_ui_path);
    run_command(
        Command::new("pnpm").arg("install").current_dir("../ui"), // Run npm install in the Angular submodule directory
        "Failed to install Angular UI dependencies",
    )
    .expect("pnpm install failed");

    // --- 6. Build Angular UI for Production ---
    println!("cargo:info=Building Angular UI for production (ng build)...");
    run_command(
        Command::new("pnpm")
            .arg("build")
            .arg("--configuration=production")
            // Output Angular build to a temporary directory within the submodule
            //.arg("--output-path")
            //.arg("dist")
            .current_dir("../ui"), // Run ng build in the Angular submodule directory
        "Failed to build Angular UI",
    )
    .expect("ng build failed");

    // --- 7. Copy Built Angular Assets ---
    println!("cargo:info=Copying Angular UI assets to Rust static directory...");

    let mut options = CopyOptions::new();
    options.overwrite = true; // Overwrite existing files in the destination
    options.content_only = true; // Copy the *contents* of angular_dist_browser_path into static_output_dir

    copy("../ui/dist/track-view.ng/browser", UI_TARGET, &options).unwrap();

    // --- 8. Clean Up Temporary Build Directory ---
    println!("cargo:info=Cleaning up temporary Angular build directory...");
    if Path::new("../ui/dist").exists() {
        fs::remove_dir_all(Path::new("../ui/dist")).unwrap();
    }

    println!("cargo:info=Angular UI build complete!");
}
// --- Helper Functions ---

// Helper function to check if a command exists and is executable
fn check_command(cmd: &str, error_msg: &str) -> Result<(), String> {
    Command::new(cmd)
        .arg("--version") // A common argument to check if a command exists and is callable
        .stdout(Stdio::null()) // Suppress stdout
        .stderr(Stdio::null()) // Suppress stderr
        .output()
        .map_err(|e| format!("{error_msg}: {e}"))
        .and_then(|output| {
            if output.status.success() {
                Ok(())
            } else {
                Err(format!(
                    "{}: Command failed with status {:?}",
                    error_msg, output.status
                ))
            }
        })
}

// Helper function to run a command and check its success
fn run_command(command: &mut Command, error_msg: &str) -> Result<(), String> {
    let output = command
        .output()
        .map_err(|e| format!("{}: {}", error_msg, e))?;

    if output.status.success() {
        // Print stdout to Cargo's warning stream for visibility during build
        println!("cargo:warning={}", String::from_utf8_lossy(&output.stdout));
        Ok(())
    } else {
        // Print stderr to Cargo's warning stream for error details
        eprintln!(
            "cargo:warning=Error: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Err(format!(
            "{}: Command failed with status {:?}",
            error_msg, output.status
        ))
    }
}
