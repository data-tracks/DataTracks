use std::cmp::PartialEq;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Add, Div, Mul, Sub};

use crate::value::array::Array;
use crate::value::dict::Dict;
use crate::value::r#type::ValType;
use crate::value::string::Text;
use crate::value::{bool, Bool, Float, Int};
use json::{parse, JsonValue};
use tracing::warn;

#[derive(Eq, Clone, Debug)]
pub enum Value {
    Int(Int),
    Float(Float),
    Bool(Bool),
    Text(Text),
    Array(Array),
    Dict(Dict),
    Null,
}

impl Value {
    pub fn text(string: &str) -> Value {
        Value::Text(Text(string.parse().unwrap()))
    }
    pub fn int(int: i64) -> Value {
        Value::Int(Int(int))
    }

    pub fn float(float: f64) -> Value {
        Value::Float(Float::new(float))
    }

    pub fn bool(bool: bool) -> Value {
        Value::Bool(Bool(bool))
    }

    pub fn array(tuple: Vec<Value>) -> Value {
        Value::Array(Array::new(tuple))
    }

    pub(crate) fn dict(values: BTreeMap<String, Value>) -> Value {
        Value::Dict(Dict::new(values))
    }

    pub(crate) fn dict_from_pair(key: String, value: Value) -> Value {
        let mut map = BTreeMap::new();
        map.insert(key, value);
        Value::Dict(Dict::new(map))
    }

    pub fn null() -> Value {
        Value::Null
    }

    pub fn type_(&self) -> ValType {
        match self {
            Value::Int(_) => ValType::Integer,
            Value::Float(_) => ValType::Float,
            Value::Bool(_) => ValType::Bool,
            Value::Text(_) => ValType::Text,
            Value::Array(_) => ValType::Array,
            Value::Dict(_) => ValType::Dict,
            Value::Null => ValType::Null
        }
    }

    pub fn as_int(&self) -> Result<Int, ()> {
        match self {
            Value::Int(i) => Ok(*i),
            Value::Float(f) => Ok(Int(f.as_f64() as i64)),
            Value::Bool(b) => Ok(if b.0 { Int(1) } else { Int(0) }),
            Value::Text(t) => t.0.parse::<i64>().map(Int).map_err(|_| ()),
            Value::Array(_) => Err(()),
            Value::Dict(_) => Err(()),
            Value::Null => Err(())
        }
    }

    pub fn as_float(&self) -> Result<Float, ()> {
        match self {
            Value::Int(i) => Ok(Float::new(i.0 as f64)),
            Value::Float(f) => Ok(*f),
            Value::Bool(b) => Ok(if b.0 { Float::new(1f64) } else { Float::new(0f64) }),
            Value::Text(t) => t.0.parse::<f64>().map(Float::new).map_err(|_| ()),
            Value::Array(_) => Err(()),
            Value::Dict(_) => Err(()),
            Value::Null => Err(())
        }
    }

    pub fn as_dict(&self) -> Result<Dict, ()> {
        match self {
            Value::Int(_) | Value::Float(_) | Value::Bool(_) | Value::Text(_) | Value::Array(_) | Value::Null => Err(()),
            Value::Dict(d) => Ok(d.clone()),
        }
    }

    pub fn as_array(&self) -> Result<Array, ()> {
        match self {
            Value::Int(_) | Value::Float(_) | Value::Bool(_) | Value::Text(_) | Value::Dict(_) | Value::Null => Err(()),
            Value::Array(a) => Ok(a.clone()),
        }
    }
    pub fn as_bool(&self) -> Result<Bool, ()> {
        match self {
            Value::Int(i) => Ok(if i.0 > 0 { Bool(true) } else { Bool(false) }),
            Value::Float(f) => Ok(if f.number <= 0 { Bool(false) } else { Bool(true) }),
            Value::Bool(b) => Ok(b.clone()),
            Value::Text(t) => {
                match t.0.to_lowercase().trim() {
                    "true" | "1" => Ok(Bool(true)),
                    _ => Ok(Bool(false))
                }
            }
            Value::Array(a) => Ok(Bool(!a.0.is_empty())),
            Value::Dict(d) => Ok(Bool(!d.0.is_empty())),
            Value::Null => Ok(Bool(false)),
        }
    }

    pub fn as_text(&self) -> Result<Text, ()> {
        match self {
            Value::Int(i) => Ok(Text(i.0.to_string())),
            Value::Float(f) => Ok(Text(f.as_f64().to_string())),
            Value::Bool(b) => Ok(Text(b.0.to_string())),
            Value::Text(t) => Ok(t.clone()),
            Value::Array(a) => Ok(Text(format!("[{}]", a.0.iter().map(|v| v.as_text().unwrap().0).collect::<Vec<String>>().join(",")))),
            Value::Dict(d) => Ok(Text(format!("[{}]", d.0.iter().map(|(k, v)| format!("{}:{}", k, v.as_text().unwrap().0)).collect::<Vec<String>>().join(",")))),
            Value::Null => Ok(Text("null".to_owned()))
        }
    }
}

// Define the macro
#[macro_export]
macro_rules! value_display {
    ($type:ty) => {
        impl std::fmt::Display for $type {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}


impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        warn!("{} == {}", self, other);

        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Null, _) | (_, Value::Null) => false,
            (Value::Int(i), Value::Int(other_i)) => i.0 == other_i.0,
            (Value::Int(_), Value::Float(_)) => other == &Value::Float(self.as_float().unwrap()),
            (Value::Float(_), Value::Int(_)) => self == &Value::Float(other.as_float().unwrap()),
            (Value::Float(f), Value::Float(other_f)) => {
                let a = f.normalize();
                let b = other_f.normalize();
                a.number == b.number && a.shift == b.shift
            }

            (Value::Bool(b), _) => other.as_bool().map(|other| other.0 == b.0).unwrap_or(false),

            (Value::Text(t), _) => other.as_text().map(|other| other.0 == t.0).unwrap_or(false),

            (Value::Array(a), _) => other.as_array().map(|other| {
                a.0.len() == other.0.len() && a.0.iter().zip(other.0.iter()).all(|(a, b)| a == b)
            }).unwrap_or(false),

            (Value::Dict(d), _) => other.as_dict().map(|other| {
                d.0.len() == other.0.len() && d.0.keys().eq(other.0.keys()) && d.0.values().eq(other.0.values())
            }).unwrap_or(false),

            // Fallback: not equal if types don't match or other cases aren't handled
            (_, _) => false,
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Int(i) => {
                i.0.hash(state);
            }
            Value::Float(f) => {
                state.write_i64(f.number);
                state.write_u8(f.shift);
            }
            Value::Bool(b) => {
                b.0.hash(state);
            }
            Value::Text(t) => {
                t.0.hash(state);
            }
            Value::Array(a) => {
                for val in &a.0 {
                    val.hash(state)
                }
            }
            Value::Dict(d) => {
                for (k, v) in &d.0 {
                    k.hash(state);
                    v.hash(state);
                }
            }
            Value::Null => {
                "null".hash(state);
            }
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(i) => i.fmt(f),
            Value::Float(float) => float.fmt(f),
            Value::Bool(b) => b.fmt(f),
            Value::Text(t) => t.fmt(f),
            Value::Array(a) => a.fmt(f),
            Value::Dict(d) => d.fmt(f),
            Value::Null => write!(f, "null")
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

impl From<Dict> for Value {
    fn from(value: Dict) -> Self {
        Value::Dict(value)
    }
}

impl Value {
    pub(crate) fn from_json(value: &str) -> Self {
        let json = parse(value);
        let mut values = BTreeMap::new();
        match json {
            Ok(json) => {
                for (key, value) in json.entries() {
                    values.insert(key.into(), value.into());
                }
            }
            Err(_) => panic!("Could not parse Dict")
        }
        Value::dict(values)
    }
}

impl From<&JsonValue> for Value {
    fn from(value: &JsonValue) -> Self {
        match value {
            JsonValue::Null => Value::null(),
            JsonValue::Short(a) => Value::text(a.as_str()),
            JsonValue::String(a) => Value::text(a),
            JsonValue::Number(a) => Value::int(a.as_fixed_point_i64(0).unwrap()),
            JsonValue::Boolean(a) => Value::bool(*a),
            JsonValue::Object(elements) => {
                Value::dict(elements.iter().map(|(k, v)| (k.to_string(), v.into())).collect())
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
            Value::Int(a) => {
                match rhs {
                    Value::Int(b) => Value::Int(*a + *b),
                    Value::Float(b) => Value::Float(*a + *b),
                    i => panic!("Cannot add {}.", i)
                }
            }
            Value::Float(a) => {
                match rhs {
                    Value::Int(b) => Value::Float(*b + *a),
                    Value::Float(b) => Value::Float(*b + *a),
                    i => panic!("Cannot add {}.", i)
                }
            }
            _ => {
                panic!("Cannot add.")
            }
        }
    }
}

impl Sub for &Value {
    type Output = Value;

    fn sub(self, _rhs: Self) -> Self::Output {
        match self {
            _ => todo!()
        }
    }
}

impl Mul for &Value {
    type Output = Value;

    fn mul(self, _rhs: Self) -> Self::Output {
        match self {
            _ => todo!()
        }
    }
}

impl Div for &Value {
    type Output = Value;

    fn div(self, _rhs: Self) -> Self::Output {
        match self {
            _ => todo!()
        }
    }
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