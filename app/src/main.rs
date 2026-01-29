extern crate core;

use crate::management::Manager;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod management;
mod util;

mod phases;

fn main() {
    setup_logging();
    util::logo();

    let manager = Manager::new();
    manager.start().unwrap();
}

fn setup_logging() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
