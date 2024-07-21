use std::cmp::PartialEq;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Add;

use json::{JsonValue, parse};

use crate::value::{HoBool, HoFloat, HoInt};
use crate::value::array::HoArray;
use crate::value::dict::HoDict;
use crate::value::null::HoNull;
use crate::value::string::HoString;
use crate::value::ValType::Integer;
use crate::value::Value::{Array, Bool, Dict, Float, Int, Null, Text};

#[derive(PartialEq, Debug, Clone)]
pub enum ValType {
    Integer,
    Float,
    Text,
    Bool,
    Tuple,
    Dict,
    Null,
    Any
}

impl ValType {
    pub(crate) fn parse(stencil: &str) -> ValType {
        match stencil.to_lowercase().as_str() {
            "int" | "integer" | "i" => Integer,
            "float" | "f" => ValType::Float,
            "bool" | "boolean" | "b" => ValType::Bool,
            "text" | "string" | "s" => ValType::Text,
            _ => panic!("Could not parse the type of the value.")
        }
    }
}

#[derive(Eq, Hash, Clone, Debug)]
pub enum Value {
    Int(HoInt),
    Float(HoFloat),
    Bool(HoBool),
    Text(Box<HoString>),
    Array(HoArray),
    Dict(HoDict),
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

    pub fn array(tuple: Vec<Value>) -> Value {
        Array(HoArray::new(tuple))
    }

    fn dict(values: BTreeMap<String, Value>) -> Value {
        Dict(HoDict::new(values))
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
                matches!(other, Null(_))
            }
            Array(a) => {
                match other {
                    Array(b) => b == a,
                    _ => false
                }
            }
            Dict(a) => {
                match other {
                    Dict(b) => a == b,
                    _ => false
                }
            }
        }
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::int(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::float(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::text(value)
    }
}

impl From<bool> for Value {

    fn from(value: bool) -> Self {
        Value::bool(value)
    }
}

impl Value {
    pub(crate) fn from_json(value: &str) -> Self {
        let json = parse(value);
        let mut values = BTreeMap::new();
        match json {
            Ok(json) => {
                for (key, value) in json.entries() {
                    values.insert(key.into(), value.into() );
                }
            }
            Err(_) => panic!("Could not parse Dict")
        }
        Value::dict(values)
    }
}

impl From<&JsonValue> for Value{
    fn from(value: &JsonValue) -> Self {
        match value {
            JsonValue::Null => Value::null(),
            JsonValue::Short(a) => Value::text(a.as_str()),
            JsonValue::String(a) => Value::text(a),
            JsonValue::Number(a) => Value::int(a.as_fixed_point_i64(0).unwrap()),
            JsonValue::Boolean(a) => Value::bool(*a),
            JsonValue::Object(elements) => {
                Value::dict(elements.iter().map(|(k,v)| (k.to_string(), v.into())).collect())
            }
            JsonValue::Array(elements) => {
                Value::array(elements.iter().map(|arg0: &JsonValue| arg0.into()).collect())
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
    fn value_equality() {
        assert_eq!(Value::int(42), Value::int(42));
        assert_ne!(Value::int(42), Value::int(7));

        assert_eq!(Value::float(3.14), Value::float(3.14));
        assert_ne!(Value::float(3.14), Value::float(2.71));

        assert_eq!(Value::bool(true), Value::bool(true));
        assert_ne!(Value::bool(true), Value::bool(false));

        assert_eq!(Value::text("Hello"), Value::text("Hello"));
        assert_ne!(Value::text("Hello"), Value::text("World"));

        assert_eq!(Value::array(vec![3.into(), 5.5.into()]), Value::array(vec![3.into(), 5.5.into()]));
        assert_ne!(Value::array(vec![5.5.into()]), Value::array(vec![3.into(), 5.5.into()]));
        assert_ne!(Value::array(vec![3.into(), 5.5.into()]), Value::array(vec![5.5.into(), 3.into()]));

        assert_eq!(Value::null(), Value::null());
    }

    #[test]
    fn value_in_vec() {
        let values = vec![
            Value::int(42),
            Value::float(3.14),
            Value::bool(true),
            Value::text("Hello"),
            Value::null(),
            Value::array(vec![3.into(), 7.into()]),
        ];

        assert_eq!(values[0], Value::int(42));
        assert_eq!(values[1], Value::float(3.14));
        assert_eq!(values[2], Value::bool(true));
        assert_eq!(values[3], Value::text("Hello"));
        assert_eq!(values[4], Value::null());
        assert_eq!(values[5], Value::array(vec![3.into(), 7.into()]));
    }

    #[test]
    fn value_in_map() {
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
        let values = vec![Value::int(3), Value::int(5), Value::float(3.3), Value::text("test"), Value::bool(false), Value::array(vec![3.into(), 7.into()])];

        for (i, raw) in raws.iter().enumerate() {
            assert_eq!(raw, &values[i])
        }
    }
}