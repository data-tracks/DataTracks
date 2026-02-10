use std::process::Command;

fn main() {
    let schema_path = "../flatbuffer/value.fbs"; // Path to your .fbs file

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
}