use crate::value::{HoFloat, HoInt, HoString, Value};

mod value;

fn main() {
    let int_a = Value::Int(HoInt(10));
    let int_b = Value::Int(HoInt(5));

    let float_a = Value::Float(HoFloat(10.5));
    let float_b = Value::Float(HoFloat(5.5));

    let string_a = Value::String(Box::new(HoString("test".parse().unwrap())));

    let values: Vec<Value> = vec![
        int_a,
        int_b,
        float_a,
        float_b,
        string_a
    ];

    for val in &values {
        println!("{} is value", val);
    }

}

