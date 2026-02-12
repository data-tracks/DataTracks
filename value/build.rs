use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    let schema_path = "../flatbuffer/value.fbs"; // Path to your .fbs file
    let generated_file = "src/value_generated.rs"; // Path to the file flatc creates

    // Tell Cargo to rerun this script if the schema changes
    println!("cargo:rerun-if-changed={}", schema_path);

    // Run the flatc compiler
    let status = Command::new("flatc")
        .args([
            "--rust",
            "-o", "src/", // Output directory
            schema_path
        ])
        .status()
        .expect("Failed to execute flatc. Is it installed?");

    // Generate TypeScript code for the frontend
    // Adjust the output path to point to your frontend folder
    Command::new("flatc")
        .args(["--ts", "-o", "../dashboard/src/generated/", schema_path])
        .status()
        .expect("flatc failed for TypeScript");

    if !status.success() {
        panic!("flatc compilation failed");
    }

    // 2. Prepends the "ignore" lines to the generated Rust file
    if Path::new(generated_file).exists() {
        let content = fs::read_to_string(generated_file).unwrap();
        // Add allows for unsafe code and unused imports often found in generated code
        let new_content = format!(
            "#![allow(unsafe_code)]\n#![allow(unused_imports)]\n#![allow(clippy::all)]\n{}",
            content
        );
        fs::write(generated_file, new_content).unwrap();
    }

    // 3. Generate TypeScript code
    Command::new("flatc")
        .args(["--ts", "-o", "../dashboard/src/generated/", schema_path])
        .status()
        .expect("flatc failed for TypeScript");
}