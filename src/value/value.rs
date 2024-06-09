use std::fmt::Display;
use std::fmt::Formatter;

use crate::value::{HoBool, HoFloat, HoInt};
use crate::value::string::HoString;

pub enum ValType {
    Integer,
    Float,
    String,
    Bool,
}

pub enum Value {
    Int(HoInt),
    Float(HoFloat),
    Bool(HoBool),
    String(Box<HoString>),
}

// Define the macro
macro_rules! value_display {
    ($type:ty) => {
        impl std::fmt::Display for $type {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

pub(crate) use value_display;


impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(v) => v.fmt(f),
            Value::Float(v) => v.fmt(f),
            Value::String(v) => v.fmt(f),
            Value::Bool(v) => v.fmt(f),
        }
    }
}


pub trait Valuable {
    fn type_(&self) -> ValType;
}

#[cfg(test)]
mod tests {
    use crate::value::{HoBool, HoFloat, HoInt, HoString, Value};

    #[test]
    fn list() {
        let int_a = Value::Int(HoInt(10));
        let int_b = Value::Int(HoInt(5));

        let float_a = Value::Float(HoFloat(10.5));
        let float_b = Value::Float(HoFloat(5.5));

        let string_a = Value::String(Box::new(HoString(String::from("test"))));

        let bool_a = Value::Bool(HoBool(true));

        let values: Vec<Value> = vec![
            int_a,
            int_b,
            float_a,
            float_b,
            string_a,
            bool_a,
        ];

        for val in &values {
            println!("{} is value", val);
        }
    }
}