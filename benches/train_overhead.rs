use criterion::{criterion_group, criterion_main, Criterion};
use data_tracks::new_channel;
use data_tracks::processing::{Block, Sender, Train};
use data_tracks::value::{Dict, Value};

pub fn benchmark_overhead(c: &mut Criterion) {
    c.bench_function("block_overhead", |b| {
        let (tx, _, rx) = new_channel();

        let sender = Sender::new(0, tx);

        let process = Box::new(move |trains: &mut Vec<Train>| {
            trains.clone().into()
        });
        let mut block = Block::new(vec![], vec![], process, sender);

        let train = Train::new(vec![Value::Dict(Dict::from(Value::int(3)))]);

        b.iter(|| {
            block.next(train.clone());

            rx.recv().unwrap();
        });
    });
}

criterion_group!(benches, benchmark_overhead);
criterion_main!(benches);
