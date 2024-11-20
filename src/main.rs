extern crate core;

use crate::management::Manager;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::thread;
use std::time::Duration;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod value;
mod ui;
mod util;
mod processing;
mod language;
mod simulation;
mod algebra;
mod management;
mod http;
mod mqtt;
mod analyse;
mod sql;
mod optimize;

fn main() {
    setup_logging();
    util::logo();

    // Create a channel to signal shutdown
    let (tx, rx) = mpsc::channel();

    // Set up the Ctrl+C handler
    ctrlc::set_handler(move || {
        tx.send(()).expect("Could not send signal on shutdown.");
    }).expect("Error setting Ctrl-C handler");

    let mut manager = Manager::new();

    manager.start();

    shutdown_hook(rx, manager)
}


fn shutdown_hook(rx: Receiver<()>, mut manager: Manager) {
    // Wait for the shutdown signal or the thread to finish
    loop {
        if rx.try_recv().is_ok() {
            info!("Received shutdown signal.");
            break;
        }

        // Sleep for a short duration to prevent busy-waiting
        thread::sleep(Duration::from_millis(100));
    }
    manager.shutdown();

    println!("Exiting main function.");
}

fn setup_logging() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");
}

