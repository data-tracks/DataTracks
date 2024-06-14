use std::cmp::PartialEq;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Add;

use crate::value::{HoBool, HoFloat, HoInt};
use crate::value::map::HoMap;
use crate::value::null::HoNull;
use crate::value::string::HoString;
use crate::value::tuple::HoTuple;
use crate::value::Value::{Bool, Float, Int, Map, Null, Text, Tuple};

pub enum ValType {
    Integer,
    Float,
    Text,
    Bool,
    Tuple,
    Map,
    Null,
}

#[derive(Eq, Hash, Clone, Debug)]
pub enum Value {
    Int(HoInt),
    Float(HoFloat),
    Bool(HoBool),
    Text(Box<HoString>),
    Tuple(HoTuple),
    Map(HoMap),
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
        Float(HoFloat::new(float))
    }

    pub fn bool(bool: bool) -> Value {
        Bool(HoBool(bool))
    }

    pub fn tuple(tuple: Vec<Value>) -> Value {
        Tuple(HoTuple::new(tuple))
    }

    pub fn map(map: HashMap<Value, Value>) -> Value {
        Map(HoMap::new(map))
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
            Tuple(v) => v.fmt(f),
            Null(v) => v.fmt(f),
            Map(v) => v.fmt(f)
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
                    _ => false
                }
            }
            Float(a) => {
                match other {
                    Int(b) => a == b,
                    Float(b) => a == b,
                    Bool(b) => a == b,
                    Text(b) => a == b,
                    _ => false
                }
            }
            Bool(a) => {
                match other {
                    Int(b) => a == b,
                    Float(b) => a == b,
                    Bool(b) => a == b,
                    Text(b) => a == b,
                    _ => false
                }
            }
            Text(a) => {
                match other {
                    Int(b) => a == b,
                    Float(b) => a == b,
                    Bool(b) => a == b,
                    Text(b) => a == b,
                    _ => false
                }
            }
            Null(_) => {
                match other {
                    Null(_) => true,
                    _ => false
                }
            }
            Tuple(a) => {
                match other {
                    Tuple(b) => b == a,
                    _ => false
                }
            }
            Map(a) => {
                match other {
                    Map(b) => b == a,
                    _ => false
                }
            }
        }
    }
}

impl Into<Value> for i64 {
    fn into(self) -> Value {
        Value::int(self)
    }
}

impl Into<Value> for f64 {
    fn into(self) -> Value {
        Value::float(self)
    }
}

impl Into<Value> for &str {
    fn into(self) -> Value {
        Value::text(self)
    }
}

impl Into<Value> for bool {
    fn into(self) -> Value {
        Value::bool(self)
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

        assert_eq!(Value::tuple(vec![3.into(), 5.5.into()]), Value::tuple(vec![3.into(), 5.5.into()]));
        assert_ne!(Value::tuple(vec![5.5.into()]), Value::tuple(vec![3.into(), 5.5.into()]));
        assert_ne!(Value::tuple(vec![3.into(), 5.5.into()]), Value::tuple(vec![5.5.into(), 3.into()]));

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
            Value::tuple(vec![3.into(), 7.into()]),
        ];

        assert_eq!(values[0], Value::int(42));
        assert_eq!(values[1], Value::float(3.14));
        assert_eq!(values[2], Value::bool(true));
        assert_eq!(values[3], Value::text("Hello"));
        assert_eq!(values[4], Value::null());
        assert_eq!(values[5], Value::tuple(vec![3.into(), 7.into()]));
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

    #[test]
    fn into() {
        let raws: Vec<Value> = vec![3.into(), 5.into(), 3.3.into(), "test".into(), false.into(), vec![3.into(), 7.into()].into()];
        let values = vec![Value::int(3), Value::int(5), Value::float(3.3), Value::text("test"), Value::bool(false), Value::tuple(vec![3.into(), 7.into()])];

        for (i, raw) in raws.iter().enumerate() {
            assert_eq!(raw, &values[i])
        }
    }
}