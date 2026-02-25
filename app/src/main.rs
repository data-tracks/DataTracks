extern crate core;

use crate::management::Manager;
use ::util::{Event, InitialRecord};
use flume::Sender;
use sink::dummy::DummySink;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use value::Value;

mod management;
mod util;

mod phases;

fn main() {
    setup_logging();
    util::logo();

    let manager = Manager::new();
    manager.start(setup_inputs).unwrap();
}

fn setup_logging() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

fn setup_inputs(joins: &mut JoinSet<()>, tx: Sender<InitialRecord>, statistics_tx: Sender<Event>) {
    let amount = 600;//8_00; // 8_00 equals entry every 3ms (16 workers per engine) // 1_000 every 1.6ms

    for i in 0..amount {
        let tx = tx.clone();
        let statistics = statistics_tx.clone();
        joins.spawn(async move {
            let mut dummy = DummySink::interval(
                Value::array(vec![Value::text("David"), Value::int(31)]),
                Duration::from_millis(10),
            );
            dummy
                .start(i, String::from("relational"), tx, statistics.clone())
                .await;
        });
    }

    for i in 0..amount {
        let tx = tx.clone();
        let statistics = statistics_tx.clone();
        joins.spawn(async move {
            let mut dummy = DummySink::interval(
                Value::dict_from_pairs(vec![
                    ("test", Value::text("test")),
                    ("key2", Value::text("test2")),
                ]),
                Duration::from_millis(10),
            );
            dummy
                .start(i, String::from("doc"), tx, statistics.clone())
                .await;
        });
    }

    for i in 0..amount {
        let tx = tx.clone();
        let statistics = statistics_tx.clone();
        joins.spawn(async move {
            let mut dummy = DummySink::interval(
                Value::dict_from_pairs(vec![
                    ("id", Value::text("test")),
                    ("label", Value::text("test2")),
                    (
                        "properties",
                        Value::dict_from_pairs(vec![("test", Value::text("text"))]),
                    ),
                ]),
                Duration::from_millis(10),
            );
            dummy
                .start(i, String::from("graph"), tx, statistics.clone())
                .await;
        });
    }
}
