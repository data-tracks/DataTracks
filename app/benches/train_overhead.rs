use criterion::{criterion_group, criterion_main, Criterion};
use data_tracks::processing::{Block, FunctionStep, Sender, Train};
use data_tracks::new_channel;
use value::{Dict, Value};

pub fn benchmark_overhead(c: &mut Criterion) {
    c.bench_function("block_overhead", |b| {
        let (tx, rx) = new_channel("test");

        let sender = Sender::new(0, tx);

        let process = Box::new(move |train: Train| {
            sender.send(train)
        });
        let func = Box::new(FunctionStep::new(process));
        
        let mut block = Block::new(vec![], vec![], func );

        let train = Train::new(vec![Value::Dict(Dict::from(Value::int(3)))]);

        b.iter(|| {
            block.next(train.clone());

            rx.recv().unwrap();
        });
    });
}

criterion_group!(benches, benchmark_overhead);
criterion_main!(benches);
