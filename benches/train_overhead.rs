use criterion::{criterion_group, criterion_main, Criterion};
use data_tracks::processing::{Block, Train};
use data_tracks::value::{Dict, Value};
use std::sync::mpsc::channel;

pub fn benchmark_overhead(c: &mut Criterion) {
    c.bench_function("block_overhead", |b| {
        let (tx, rx) = channel();

        let process = Box::new(move |trains: &mut Vec<Train>| {
            tx.send(trains.clone()).unwrap();
        });
        let mut block = Block::new(vec![], vec![], process);

        let train = Train::new(0, vec![Value::Dict(Dict::from(Value::int(3)))]);

        b.iter(|| {
            block.next(train.clone());

            rx.recv().unwrap();
        });
    });
}

criterion_group!(benches, benchmark_overhead);
criterion_main!(benches);
