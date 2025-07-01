use std::process::{Command, Stdio};
use std::env;
use std::path::{Path, PathBuf};
use fs_extra::dir::{copy, CopyOptions}; // Requires 'fs_extra' crate in build-dependencies

fn main() {
    // --- 1. Configure Cargo Rerun Conditions ---
    // Tell Cargo to re-run this build script if any of these files/directories change.
    // This helps ensure the Angular UI is rebuilt when its configuration or dependencies change.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=ui/package.json");
    println!("cargo:rerun-if-changed=ui/angular.json");
    // You might want to add more specific directories if your Angular build is sensitive
    // to changes in certain source files, e.g., println!("cargo:rerun-if-changed=ui/angular-app/src");

    // --- 2. Define Paths ---
    // Get the path to the root of the Rust project (where Cargo.toml is located)
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR not set");
    let project_root = PathBuf::from(cargo_manifest_dir);

    // Path to the Angular UI submodule
    let angular_ui_path = project_root.join("ui");

    // Determine the target directory for the Rust build (e.g., target/debug or target/release)
    // OUT_DIR is typically target/{profile}/build/{crate_name}-{hash}/out
    // We need to go up a few levels to get to target/{profile}
    let rust_target_profile_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"))
        .parent().expect("Failed to get parent of OUT_DIR") // up from 'out'
        .parent().expect("Failed to get parent of build-hash dir") // up from '{crate_name}-{hash}'
        .parent().expect("Failed to get parent of 'build' dir") // up from 'build'
        .to_path_buf();

    // Define the final destination for Angular static files within the Rust target directory
    let static_output_dir = rust_target_profile_dir.join(env::var("PROFILE").expect("PROFILE not set")).join("static");

    // Define a temporary directory for Angular's build output before copying
    let angular_temp_dist_dir = angular_ui_path.join("dist_build_rs_temp");

    // --- 3. Create Output Directory ---
    // Ensure the final static output directory exists
    std::fs::create_dir_all(&static_output_dir)
        .expect(&format!("Failed to create static output directory at {:?}", static_output_dir));

    // --- 4. Check for External Dependencies (Node.js, npm, Angular CLI) ---
    println!("cargo:warning=Checking for Node.js, npm, and Angular CLI...");
    check_command("node", "Node.js is not installed or not in PATH. Please install Node.js (which includes npm) to build the Angular UI.")
        .expect("Node.js check failed");
    check_command("pnpm", "npm is not installed or not in PATH. Please install Node.js (which includes npm) to build the Angular UI.")
        .expect("npm check failed");
    check_command("ng", "Angular CLI ('ng') is not installed globally. Run 'npm install -g @angular/cli'.")
        .expect("Angular CLI check failed");

    // --- 5. Install Angular Dependencies ---
    println!("cargo:warning=Installing Angular UI dependencies (npm install)...");
    run_command(
        Command::new("pnpm")
            .arg("install")
            .current_dir(&angular_ui_path), // Run npm install in the Angular submodule directory
        "Failed to install Angular UI dependencies",
    ).expect("pnpm install failed");

    // --- 6. Build Angular UI for Production ---
    println!("cargo:warning=Building Angular UI for production (ng build)...");
    run_command(
        Command::new("ng")
            .arg("build")
            .arg("--configuration=production")
            // Output Angular build to a temporary directory within the submodule
            .arg("--output-path")
            .arg(&angular_temp_dist_dir)
            .current_dir(&angular_ui_path), // Run ng build in the Angular submodule directory
        "Failed to build Angular UI",
    ).expect("ng build failed");

    // --- 7. Copy Built Angular Assets ---
    println!("cargo:warning=Copying Angular UI assets to Rust static directory...");
    // The default Angular build output path is typically `dist/<project-name>/browser/`
    // You might need to adjust "angular-app" if your Angular project name is different in angular.json
    let angular_dist_browser_path = angular_temp_dist_dir.join("browser");

    let mut options = CopyOptions::new();
    options.overwrite = true; // Overwrite existing files in the destination
    options.copy_inside = true; // Copy the *contents* of angular_dist_browser_path into static_output_dir

    copy(&angular_dist_browser_path, &static_output_dir, &options)
        .expect(&format!("Failed to copy Angular UI assets from {:?} to {:?}", angular_dist_browser_path, static_output_dir));

    // --- 8. Clean Up Temporary Build Directory ---
    println!("cargo:warning=Cleaning up temporary Angular build directory...");
    if angular_temp_dist_dir.exists() {
        std::fs::remove_dir_all(&angular_temp_dist_dir)
            .expect(&format!("Failed to remove temporary Angular build directory at {:?}", angular_temp_dist_dir));
    }

    println!("cargo:warning=Angular UI build and copy process complete!");
}

// --- Helper Functions ---

// Helper function to check if a command exists and is executable
fn check_command(cmd: &str, error_msg: &str) -> Result<(), String> {
    Command::new(cmd)
        .arg("--version") // A common argument to check if a command exists and is callable
        .stdout(Stdio::null()) // Suppress stdout
        .stderr(Stdio::null()) // Suppress stderr
        .output()
        .map_err(|e| format!("{}: {}", error_msg, e))
        .and_then(|output| {
            if output.status.success() {
                Ok(())
            } else {
                Err(format!("{}: Command failed with status {:?}", error_msg, output.status))
            }
        })
}

// Helper function to run a command and check its success
fn run_command(command: &mut Command, error_msg: &str) -> Result<(), String> {
    let output = command.output()
        .map_err(|e| format!("{}: {}", error_msg, e))?;

    if output.status.success() {
        // Print stdout to Cargo's warning stream for visibility during build
        println!("cargo:warning={}", String::from_utf8_lossy(&output.stdout));
        Ok(())
    } else {
        // Print stderr to Cargo's warning stream for error details
        eprintln!("cargo:warning=Error: {}", String::from_utf8_lossy(&output.stderr));
        Err(format!("{}: Command failed with status {:?}", error_msg, output.status))
    }
}
