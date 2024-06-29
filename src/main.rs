extern crate core;

use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use crate::ui::start;

mod value;
mod ui;
mod util;
mod processing;
mod language;
mod simulation;
mod algebra;
mod mangagement;

fn main() {
    setup_logging();
    util::logo();

    // Create a channel to signal shutdown
    let (tx, rx) = mpsc::channel();

    // Set up the Ctrl+C handler
    ctrlc::set_handler(move || {
        tx.send(()).expect("Could not send signal on shutdown.");
    }).expect("Error setting Ctrl-C handler");

    let storage = mangagement::start();

    // Spawn a new thread
    let handle = thread::spawn(|| start(storage));


    shutdown_hook(rx, handle);
}

fn shutdown_hook(rx: Receiver<()>, handle: JoinHandle<()>) {
// Wait for the shutdown signal or the thread to finish
    loop {
        if rx.try_recv().is_ok() {
            println!("Received shutdown signal.");
            break;
        }

        if handle.is_finished() {
            println!("Thread finished.");
            handle.join().unwrap();
            break;
        }

        // Sleep for a short duration to prevent busy-waiting
        thread::sleep(Duration::from_millis(100));
    }

    println!("Exiting main function.");
}

fn setup_logging() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");
}

