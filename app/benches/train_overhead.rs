use criterion::{criterion_group, criterion_main, Criterion};
use data_tracks::algebra::{Executor, IdentityIterator};
use data_tracks::new_channel;
use data_tracks::processing::{Sender, Train};
use value::{Dict, Value};

pub fn benchmark_overhead(c: &mut Criterion) {
    c.bench_function("block_overhead", |b| {
        let (tx, rx) = new_channel("test");

        let sender = Sender::new(0, tx);

        let handler = Box::new(move |train: Train| sender.send(train));

        let identity = Box::new(IdentityIterator::new());

        let executer = Executor::new(0, identity, handler);

        let train = Train::new(vec![Value::Dict(Dict::from(Value::int(3)))]);

        b.iter(|| {
            //block.next(train.clone());

            rx.recv().unwrap();
        });
    });
}

criterion_group!(benches, benchmark_overhead);
criterion_main!(benches);
