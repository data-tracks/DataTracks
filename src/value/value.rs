use std::cmp::PartialEq;
use std::fmt::{Display};
use std::fmt::Formatter;
use std::ops::Add;

use crate::value::{HoBool, HoFloat, HoInt};
use crate::value::string::HoString;
use crate::value::Value::{Bool, Float, Int, Null, Text};

pub enum ValType {
    Integer,
    Float,
    Text,
    Bool,
    Null
}

#[derive(Clone, Debug)]
pub enum Value {
    Int(HoInt),
    Float(HoFloat),
    Bool(HoBool),
    Text(Box<HoString>),
    Null(HoNull)
}


impl Value {
    pub fn text(string: &str) -> Value {
        Text(Box::new(HoString(string.parse().unwrap())))
    }
    pub fn int(int: i64) -> Value {
        Int(HoInt(int))
    }

    pub fn float(float: f64) -> Value {
        Float(HoFloat(float))
    }

    pub fn bool(bool: bool) -> Value {
        Bool(HoBool(bool))
    }

    pub fn null() -> Value {
        Null(HoNull{})
    }
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
use crate::value::null::HoNull;

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Int(v) => v.fmt(f),
            Float(v) => v.fmt(f),
            Text(v) => v.fmt(f),
            Bool(v) => v.fmt(f),
            Null(v) => v.fmt(f),
        }
    }
}


impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Int(a) => {
                match other {
                    Int(b) => a == b,
                    Float(b) => a == b,
                    Bool(b) => a == b,
                    Text(b) => a == b,
                    Null(_) => false
                }
            }
            Float(a) => {
                match other {
                    Int(b) => a == b,
                    Float(b) => a == b,
                    Bool(b) => a == b,
                    Text(b) => a == b,
                    Null(_) => false
                }
            }
            Bool(a) => {
                match other {
                    Int(b) => a == b,
                    Float(b) => a == b,
                    Bool(b) => a == b,
                    Text(b) => a == b,
                    Null(_) => false
                }
            }
            Text(a) => {
                match other {
                    Int(b) => a == b,
                    Float(b) => a == b,
                    Bool(b) => a == b,
                    Text(b) => a == b,
                    Null(_) => false,
                }
            }
            Null(_) => {
                match other {
                    Int(_) => false,
                    Float(_) => false,
                    Bool(_) => false,
                    Text(_) => false,
                    Null(_) => true
                }
            }
        }
    }
}


impl Add for &Value {
    type Output = Value;

    fn add(self, rhs: Self) -> Self::Output {
        match self {
            Int(a) => {
                match rhs {
                    Int(b) => Int(*a + *b),
                    Float(b) => Float(*a + *b),
                    _ => panic!("Cannot add.")
                }
            }
            Float(a) => {
                match rhs {
                    Int(b) => Float(*b + *a),
                    Float(b) => Float(*b + *a),
                    _ => panic!("Cannot add.")
                }
            }
            _ => {
                panic!("Cannot add.")
            }
        }
    }
}


pub trait Valuable {
    fn type_(&self) -> ValType;
}

#[cfg(test)]
mod tests {
    use crate::value::Value;

    #[test]
    fn list() {
        let int_a = Value::int(10);
        let int_b = Value::int(5);

        let float_a = Value::float(10.5);
        let float_b = Value::float(5.5);

        let string_a = Value::text("test");

        let bool_a = Value::bool(true);

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