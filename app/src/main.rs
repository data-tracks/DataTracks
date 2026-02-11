extern crate core;

use crate::management::Manager;
use flume::Sender;
use sink::dummy::DummySink;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use ::util::InitialMeta;
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

fn setup_inputs(joins: &mut JoinSet<()>, tx: Sender<(Value, InitialMeta)>) {
    let amount = 2_0;

    for _ in 0..amount {
        let tx = tx.clone();
        joins.spawn(async {
            let mut dummy = DummySink::interval(
                Value::array(vec![Value::text("David"), Value::int(31)]),
                Duration::from_millis(10),
            );
            dummy.start(String::from("relational"), tx).await;
        });
    }

    for _ in 0..amount {
        let tx = tx.clone();
        joins.spawn(async {
            let mut dummy = DummySink::interval(
                Value::dict_from_pairs(vec![
                    ("test", Value::text("test")),
                    ("key2", Value::text("test2")),
                ]),
                Duration::from_millis(10),
            );
            dummy.start(String::from("doc"), tx).await;
        });
    }

    for _ in 0..amount {
        let tx = tx.clone();
        joins.spawn(async {
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
            dummy.start(String::from("graph"), tx).await;
        });
    }
}
