extern crate core;

use crate::management::Storage;
use crate::mqtt::MqttSource;
use crate::processing::{DebugDestination, HttpSource, Plan};
use crate::ui::start;
use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tracing::log::info;
use tracing::Level;
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

fn main() {
    setup_logging();
    util::logo();

    // Create a channel to signal shutdown
    let (tx, rx) = mpsc::channel();

    // Set up the Ctrl+C handler
    ctrlc::set_handler(move || {
        tx.send(()).expect("Could not send signal on shutdown.");
    }).expect("Error setting Ctrl-C handler");

    let storage = management::start();

    add_default(storage.clone());
    // Spawn a new thread
    let handle = thread::spawn(|| start(storage));

    shutdown_hook(rx, handle);
}

fn add_default(storage: Arc<Mutex<Storage>>) {
    thread::spawn(move || {
        let mut plan = Plan::parse("1-2-3");
        plan.add_source(1, Box::new(HttpSource::new(1, 5555)));
        plan.add_source(1, Box::new(MqttSource::new(1, 6666)));
        plan.add_destination(3, Box::new(DebugDestination::new(3)));
        let id = plan.id;
        plan.set_name("Default".to_string());
        let mut lock = storage.lock().unwrap();
        lock.add_plan(plan);
        lock.start_plan(id);
        drop(lock);
    });

}

fn shutdown_hook(rx: Receiver<()>, handle: JoinHandle<()>) {
// Wait for the shutdown signal or the thread to finish
    loop {
        if rx.try_recv().is_ok() {
            info!("Received shutdown signal.");
            break;
        }

        if handle.is_finished() {
            info!("Thread finished.");
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

