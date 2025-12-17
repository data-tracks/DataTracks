use criterion::{Criterion, criterion_group, criterion_main};
use speedy::{Readable, Writable};
use value::Value;

fn get_values() -> Vec<Value> {
    vec![
        Value::bool(true),
        Value::int(1),
        Value::float(2.5),
        Value::text("hello"),
        Value::null(),
        Value::time(350, 5),
        Value::date(305),
        Value::array(vec![3.into(), 7.into()]),
        Value::dict_from_pairs(vec![("test", 7.into()), ("hi", 5.into())]),
    ]
}

fn bench_serialize(c: &mut Criterion) {
    for value in get_values() {
        c.bench_function(format!("serialize {:?}", value.type_()).as_str(), |b| {
            b.iter(|| serialize_value(&value))
        });
    }
}

fn bench_deserialize(c: &mut Criterion) {
    for value in get_values() {
        c.bench_function(format!("deserialize {:?}", value.type_()).as_str(), |b| {
            b.iter(|| deserialize_value(&serialize_value(&value)))
        });
    }
}

criterion_group!(benches, bench_serialize, bench_deserialize);
criterion_main!(benches);

fn serialize_value(value: &Value) -> Vec<u8> {
    value.write_to_vec().unwrap()
}

fn deserialize_value(value: &Vec<u8>) -> Value {
    Value::read_from_buffer(value).unwrap()
}
