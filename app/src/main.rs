extern crate core;

use crate::management::Manager;
use tokio::task::JoinSet;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod algebra;
mod analyse;
mod http;
mod language;
mod management;
mod mongo;
mod mqtt;
mod postgres;
mod processing;
mod sqlite;
mod ui;
mod util;

mod optimize;
mod tests;
mod tpc;

#[tokio::main]
async fn main() {
    setup_logging();
    util::logo();

    let mut manager = Manager::new();
    manager.start().await.unwrap()
}

fn setup_logging() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
