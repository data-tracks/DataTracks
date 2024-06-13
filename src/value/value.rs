use std::cmp::PartialEq;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Add;

use crate::value::{HoBool, HoFloat, HoInt};
use crate::value::null::HoNull;
use crate::value::string::HoString;
use crate::value::Value::{Bool, Float, Int, Null, Text};

pub enum ValType {
    Integer,
    Float,
    Text,
    Bool,
    Null,
}

#[derive(Clone, Debug)]
pub enum Value {
    Int(HoInt),
    Float(HoFloat),
    Bool(HoBool),
    Text(Box<HoString>),
    Null(HoNull),
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
        Null(HoNull {})
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
    use std::collections::HashMap;

    use crate::value::Value;

    #[test]
    fn test_value_equality() {
        assert_eq!(Value::int(42), Value::int(42));
        assert_ne!(Value::int(42), Value::int(7));

        assert_eq!(Value::float(3.14), Value::float(3.14));
        assert_ne!(Value::float(3.14), Value::float(2.71));

        assert_eq!(Value::bool(true), Value::bool(true));
        assert_ne!(Value::bool(true), Value::bool(false));

        assert_eq!(Value::text("Hello"), Value::text("Hello"));
        assert_ne!(Value::text("Hello"), Value::text("World"));

        assert_eq!(Value::null(), Value::null());
    }

    #[test]
    fn test_value_in_vec() {
        let values = vec![
            Value::int(42),
            Value::float(3.14),
            Value::bool(true),
            Value::text("Hello"),
            Value::null(),
        ];

        assert_eq!(values[0], Value::int(42));
        assert_eq!(values[1], Value::float(3.14));
        assert_eq!(values[2], Value::bool(true));
        assert_eq!(values[3], Value::text("Hello"));
        assert_eq!(values[4], Value::null());
    }

    #[test]
    fn test_value_in_map() {
        let mut map = HashMap::new();
        map.insert("int", Value::int(42));
        map.insert("float", Value::float(3.14));
        map.insert("bool", Value::bool(true));
        map.insert("text", Value::text("Hello"));
        map.insert("null", Value::null());

        assert_eq!(map.get("int"), Some(&Value::int(42)));
        assert_eq!(map.get("float"), Some(&Value::float(3.14)));
        assert_eq!(map.get("bool"), Some(&Value::bool(true)));
        assert_eq!(map.get("text"), Some(&Value::text("Hello")));
        assert_eq!(map.get("null"), Some(&Value::null()));
    }
}