extern crate core;

use crate::management::Manager;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::thread;
use std::time::Duration;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod algebra;
mod analyse;
mod http;
mod language;
mod management;
mod mqtt;
mod processing;
mod sql;
mod ui;
mod util;

mod optimize;
mod tpc;

fn main() {
    setup_logging();
    util::logo();

    // Create a channel to signal shutdown
    let (tx, rx) = mpsc::channel();

    // Set up the Ctrl+C handler
    ctrlc::set_handler(move || {
        tx.send(()).expect("Could not send signal on shutdown.");
    })
    .expect("Error setting Ctrl-C handler");

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
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
