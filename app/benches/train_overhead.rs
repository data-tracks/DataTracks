use criterion::{Criterion, criterion_group, criterion_main};

pub fn benchmark_overhead(c: &mut Criterion) {
    c.bench_function("block_overhead", |b| {
        b.iter(|| {
            //block.next(train.clone_boxed());
        });
    });
}

criterion_group!(benches, benchmark_overhead);
criterion_main!(benches);
