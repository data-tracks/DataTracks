use crate::value::{HoFloat, HoInt, HoNumber};

mod value;

fn main() {
    let int_a = HoNumber::Int(HoInt(10));
    let int_b = HoNumber::Int(HoInt(5));

    let float_a = HoNumber::Float(HoFloat(10.5));
    let float_b = HoNumber::Float(HoFloat(5.5));

    let numbers: Vec<HoNumber> = vec![
        int_a,
        int_b,
        float_a,
        float_b
    ];

    for number in &numbers {
        println!("{} has mod: {}", number, number.float());
    }

    for number in &numbers {
        println!("{} has mod: {}", number, number.int());
    }

    let mixed_add = numbers[0] + numbers[2];
    let mixed_sub = numbers[0] - numbers[3];

    println!("{}", mixed_add); // Should print "FloatWrapper(20.5)"
    println!("{}", mixed_sub); // Should print "FloatWrapper(4.5)"
}

