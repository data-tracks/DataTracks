use crate::array::Array;
use crate::date::Date;
use crate::dict::Dict;
use crate::edge::Edge;
use crate::node::Node;
use crate::r#type::ValType;
use crate::text::Text;
use crate::time::Time;
use crate::{bool, Bool, Float, Int};
use core::fmt::Pointer;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::cmp::PartialEq;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str;
use anyhow::{anyhow, bail};
use tracing::debug;


#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Readable, Writable, Default)]
pub enum Value {
    Int(Int),
    Float(Float),
    Bool(Bool),
    Text(Text),
    Time(Time),
    Date(Date),
    Array(Array),
    Dict(Dict),
    Node(Box<Node>),
    Edge(Box<Edge>),
    #[default]
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

    pub fn float_parts(number: i64, shift: u8) -> Value {
        Value::Float(Float { number, shift })
    }

    pub fn bool(bool: bool) -> Value {
        Value::Bool(Bool(bool))
    }

    pub fn time(ms: i64, ns: u32) -> Value {
        Value::Time(Time::new(ms, ns))
    }

    pub fn date(days: i64) -> Value {
        Value::Date(Date::new(days))
    }

    pub fn node(id: Int, labels: Vec<Text>, properties: BTreeMap<String, Value>) -> Value {
        Value::Node(Box::new(Node {
            id,
            labels,
            properties,
        }))
    }

    pub fn array(tuple: Vec<Value>) -> Value {
        Value::Array(Array::new(tuple))
    }

    pub fn dict(values: BTreeMap<String, Value>) -> Value {
        let mut map = BTreeMap::new();

        values.into_iter().for_each(|(k, v)| {
            match v {
                Value::Dict(d) => {
                    flatten(d, vec![k]).into_iter().for_each(|(k, v)| {
                        map.insert(k, v);
                    });
                }
                _ => {
                    map.insert(k, v);
                }
            };
        });

        Value::Dict(Dict::new(map))
    }

    pub fn dict_from_kv<S: AsRef<str>>(key: S, value: Value) -> Value {
        Self::dict_from_pairs(vec![(key.as_ref(), value)])
    }

    pub fn dict_from_pairs(pairs: Vec<(&str, Value)>) -> Value {
        let mut map = BTreeMap::new();
        pairs.into_iter().for_each(|(k, v)| {
            map.insert(k.to_string(), v);
        });
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
            Value::Null => ValType::Null,
            Value::Time(_) => ValType::Time,
            Value::Date(_) => ValType::Date,
            Value::Node(_) => ValType::Node,
            Value::Edge(_) => ValType::Edge,
        }
    }

    pub fn as_int(&self) -> anyhow::Result<Int> {
        match self {
            Value::Int(i) => Ok(*i),
            Value::Float(f) => Ok(Int(f.as_f64() as i64)),
            Value::Bool(b) => Ok(if b.0 { Int(1) } else { Int(0) }),
            Value::Text(t) => t.0.parse::<i64>().map(Int).map_err(|e| anyhow!(e)),
            Value::Array(_) => bail!("Array cannot be converted"),
            Value::Dict(_) => bail!("Dict cannot be converted"),
            Value::Null => bail!("Null cannot be converted"),
            Value::Time(t) => Ok(Int(t.ms)),
            Value::Date(d) => Ok(Int::new(d.as_epoch())),
            Value::Node(_) => bail!("Node cannot be converted"),
            Value::Edge(_) => bail!("Edge cannot be converted"),
        }
    }

    pub fn as_float(&self) -> anyhow::Result<Float> {
        match self {
            Value::Int(i) => Ok(Float::new(i.0 as f64)),
            Value::Float(f) => Ok(*f),
            Value::Bool(b) => Ok(if b.0 {
                Float::new(1f64)
            } else {
                Float::new(0f64)
            }),
            Value::Text(t) => {
                t.0.parse::<f64>()
                    .map(Float::new)
                    .map_err(|err| anyhow!(err))
            }
            Value::Array(_) => bail!("Array cannot be converted"),
            Value::Dict(_) => bail!("Dict cannot be converted"),
            Value::Null => bail!("Null cannot be converted"),
            Value::Time(t) => Ok(if t.ns != 0 {
                Float::new(t.ms as f64 + t.ns as f64 / 1e6)
            } else {
                Float::new(t.ms as f64)
            }),
            Value::Date(d) => Ok(Float::new(d.as_epoch() as f64)),
            Value::Node(_) => bail!("Node cannot be converted"),
            Value::Edge(_) => bail!("Edge cannot be converted"),
        }
    }

    pub fn as_node(&self) -> anyhow::Result<&Node> {
        match self {
            Value::Node(n) => Ok(n),
            _ => bail!("Cannot convert to Node"),
        }
    }

    pub(crate) fn as_edge(&self) -> anyhow::Result<&Edge> {
        match self {
            Value::Edge(e) => Ok(e),
            _ => bail!("Cannot convert to Edge"),
        }
    }

    pub fn as_time(&self) -> anyhow::Result<Time> {
        match self {
            Value::Int(i) => Ok(Time::new(i.0, 0)),
            Value::Float(f) => {
                let num = f.number.to_string();
                let (ms, partial_ns) = num.split_at(num.len() - f.shift as usize);
                let ns = format!("{:0<6}", partial_ns.chars().take(6).collect::<String>());
                Ok(Time::new(ms.parse().unwrap(), ns.parse().unwrap()))
            }
            Value::Bool(b) => Ok(Time::new(b.0 as i64, 0)),
            Value::Text(t) => Ok(Time::from(t.clone())),
            Value::Time(t) => Ok(*t),
            Value::Array(_) => bail!("Array cannot be converted"),
            Value::Dict(_) => bail!("Dict cannot be converted"),
            Value::Null => bail!("Null cannot be converted"),
            Value::Date(_) => Ok(Time::new(0, 0)),
            Value::Node(_) => bail!("Node cannot be converted"),
            Value::Edge(_) => bail!("Edge cannot be converted"),
        }
    }

    pub fn as_date(&self) -> anyhow::Result<Date> {
        match self {
            Value::Int(i) => Ok(Date::new(i.0)),
            Value::Float(f) => Ok(Date::new(f.as_f64() as i64)),
            Value::Bool(b) => Ok(Date::new(b.0 as i64)),
            Value::Text(t) => Ok(Date::from(t.0.clone())),
            Value::Time(_) => bail!("Time cannot be converted"),
            Value::Date(d) => Ok(d.clone()),
            Value::Array(_) => bail!("Array cannot be converted"),
            Value::Dict(_) => bail!("Dict cannot be converted"),
            Value::Null => bail!("Null cannot be converted"),
            Value::Node(_) => bail!("Node cannot be converted"),
            Value::Edge(_) => bail!("Edge cannot be converted"),
        }
    }

    pub fn as_dict(&self) -> anyhow::Result<Dict> {
        match self {
            Value::Time(_)
            | Value::Int(_)
            | Value::Float(_)
            | Value::Bool(_)
            | Value::Text(_)
            | Value::Array(_)
            | Value::Date(_)
            | Value::Node(_)
            | Value::Edge(_)
            | Value::Null => bail!("Dict cannot be converted"),
            Value::Dict(d) => Ok(d.clone()),
        }
    }

    pub fn as_array(&self) -> anyhow::Result<Array> {
        match self {
            Value::Time(_)
            | Value::Int(_)
            | Value::Float(_)
            | Value::Bool(_)
            | Value::Text(_)
            | Value::Date(_)
            | Value::Dict(_)
            | Value::Node(_)
            | Value::Edge(_)
            | Value::Null => bail!("Array cannot be converted"),
            Value::Array(a) => Ok(a.clone()),
        }
    }
    pub fn as_bool(&self) -> anyhow::Result<Bool> {
        match self {
            Value::Int(i) => Ok(if i.0 > 0 { Bool(true) } else { Bool(false) }),
            Value::Float(f) => Ok(if f.number <= 0 {
                Bool(false)
            } else {
                Bool(true)
            }),
            Value::Bool(b) => Ok(b.clone()),
            Value::Text(t) => match t.0.to_lowercase().trim() {
                "true" | "1" => Ok(Bool(true)),
                _ => Ok(Bool(false)),
            },
            Value::Time(t) => Ok(if t.ms > 0 { Bool(true) } else { Bool(false) }),
            Value::Array(a) => Ok(Bool(!a.values.is_empty())),
            Value::Dict(d) => Ok(Bool(!d.is_empty())),
            Value::Null => Ok(Bool(false)),
            Value::Date(d) => Ok(Bool::new(d.days > 0)),
            Value::Node(_) => bail!("Node cannot be converted"),
            Value::Edge(_) => bail!("Edge cannot be converted"),
        }
    }

    pub fn as_text(&self) -> anyhow::Result<Text> {
        match self {
            Value::Int(i) => Ok(Text(i.0.to_string())),
            Value::Float(f) => Ok(Text(f.as_f64().to_string())),
            Value::Bool(b) => Ok(Text(b.0.to_string())),
            Value::Text(t) => Ok(t.clone()),
            Value::Array(a) => Ok(Text(format!(
                "[{}]",
                a.values
                    .iter()
                    .map(|v| v.as_text().unwrap().0)
                    .collect::<Vec<String>>()
                    .join(",")
            ))),
            Value::Dict(d) => Ok(Text(format!(
                "[{}]",
                d.iter()
                    .map(|(k, v)| format!("{}:{}", k, v.as_text().unwrap().0))
                    .collect::<Vec<String>>()
                    .join(",")
            ))),
            Value::Null => Ok(Text("null".to_owned())),
            Value::Time(t) => Ok(Text(t.to_string())),
            Value::Date(d) => Ok(Text(d.to_string())),
            Value::Node(_) => bail!("Node cannot be converted"),
            Value::Edge(_) => bail!("Edge cannot be converted"),
        }
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::int(value as i64)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::int(value as i64)
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        Value::int(value as i64)
    }
}

fn flatten(dict: Dict, prefix: Vec<String>) -> Vec<(String, Value)> {
    let mut values = vec![];
    dict.into_iter().for_each(|(k, v)| match v {
        Value::Dict(d) => {
            let mut prefix = prefix.clone();
            prefix.push(k);
            values.append(&mut flatten(d, prefix))
        }
        _ => values.push((k, v)),
    });
    values
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

impl Eq for Value {}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        debug!("{} == {}", self, other);

        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Null, _) | (_, Value::Null) => false,
            (Value::Int(_), Value::Float(_)) => self
                .as_float()
                .map(|v| &Value::Float(v) == other)
                .unwrap_or(false),
            (Value::Int(i), _) => other.as_int().map(|v| i.0 == v.0).unwrap_or(false),
            (Value::Float(f), Value::Float(other_f)) => {
                let a = f.normalize();
                let b = other_f.normalize();
                a.number == b.number && a.shift == b.shift
            }
            (Value::Float(_), _) => other
                .as_float()
                .map(|v| self == &Value::Float(v))
                .unwrap_or(false),
            (Value::Bool(b), _) => other.as_bool().map(|other| other.0 == b.0).unwrap_or(false),

            (Value::Text(t), _) => other.as_text().map(|other| other.0 == t.0).unwrap_or(false),

            (Value::Array(a), _) => other
                .as_array()
                .map(|other| {
                    a.values.len() == other.values.len()
                        && a.values
                            .iter()
                            .zip(other.values.iter())
                            .all(|(a, b)| a == b)
                })
                .unwrap_or(false),

            (Value::Dict(d), _) => other
                .as_dict()
                .map(|other| {
                    d.len() == other.len()
                        && d.keys().eq(other.keys())
                        && d.values().eq(other.values())
                })
                .unwrap_or(false),
            (Value::Time(t1), Value::Time(t2)) => t1 == t2,
            (Value::Time(t), _) => t == &other.as_time().unwrap(),
            (Value::Date(d1), Value::Date(d2)) => d1 == d2,
            (Value::Date(d), _) => d == &other.as_date().unwrap(),
            (Value::Node(n1), Value::Node(n2)) => n1 == n2,
            (Value::Edge(n1), Value::Edge(n2)) => n1 == n2,
            (Value::Node(n), o) => o.as_node().map(|node| n.as_ref() == node).unwrap_or(false),
            (Value::Edge(e), o) => o.as_edge().map(|edge| e.as_ref() == edge).unwrap_or(false),
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
                for val in &(*a.values) {
                    val.hash(state)
                }
            }
            Value::Dict(d) => {
                for (k, v) in &d.values {
                    k.hash(state);
                    v.hash(state);
                }
            }
            Value::Null => {
                "null".hash(state);
            }
            Value::Time(t) => {
                t.ms.hash(state);
                t.ns.hash(state)
            }
            Value::Date(d) => {
                d.days.hash(state);
            }
            Value::Node(n) => {
                n.labels.hash(state);
                n.properties.hash(state)
            }
            Value::Edge(e) => {
                e.label.hash(state);
                e.properties.hash(state);
                e.start.hash(state);
                e.end.hash(state);
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
            Value::Time(t) => t.fmt(f),
            Value::Array(a) => a.fmt(f),
            Value::Dict(d) => d.fmt(f),
            Value::Null => write!(f, "null"),
            Value::Date(d) => d.fmt(f),
            Value::Node(n) => n.fmt(f),
            Value::Edge(e) => e.fmt(f),
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

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::text(&value)
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

#[cfg(test)]
mod tests {
    use crate::value::Value;
    use std::collections::hash_map::DefaultHasher;
    use std::collections::HashMap;
    use std::hash::{Hash, Hasher};
    use std::vec;

    #[test]
    fn value_equality() {
        assert_eq!(Value::int(42), Value::int(42));
        assert_ne!(Value::int(42), Value::int(7));

        assert_eq!(Value::float(3.314), Value::float(3.314));
        assert_ne!(Value::float(3.314), Value::float(2.71));

        assert_eq!(Value::bool(true), Value::bool(true));
        assert_ne!(Value::bool(true), Value::bool(false));

        assert_eq!(Value::text("Hello"), Value::text("Hello"));
        assert_ne!(Value::text("Hello"), Value::text("World"));

        assert_eq!(
            Value::array(vec![3.into(), 5.5.into()]),
            Value::array(vec![3.into(), 5.5.into()])
        );
        assert_ne!(
            Value::array(vec![5.5.into()]),
            Value::array(vec![3.into(), 5.5.into()])
        );
        assert_ne!(
            Value::array(vec![3.into(), 5.5.into()]),
            Value::array(vec![5.5.into(), 3.into()])
        );

        assert_eq!(Value::time(3000, 0), Value::time(3000, 0));
        assert_ne!(Value::time(3000, 50), Value::time(3000, 0));

        assert_eq!(Value::date(3500), Value::date(3500));
        assert_ne!(Value::date(3500), Value::date(3600));

        assert_eq!(Value::null(), Value::null());
    }

    #[test]
    fn value_in_vec() {
        let values = vec![
            Value::int(42),
            Value::float(3.314),
            Value::bool(true),
            Value::text("Hello"),
            Value::null(),
            Value::time(3, 0),
            Value::date(305),
            Value::array(vec![3.into(), 7.into()]),
        ];

        assert_eq!(values[0], Value::int(42));
        assert_eq!(values[1], Value::float(3.314));
        assert_eq!(values[2], Value::bool(true));
        assert_eq!(values[3], Value::text("Hello"));
        assert_eq!(values[4], Value::null());
        assert_eq!(values[5], Value::time(3, 0));
        assert_eq!(values[6], Value::date(305));
        assert_eq!(values[7], Value::array(vec![3.into(), 7.into()]));
    }

    #[test]
    fn value_in_map() {
        let mut map = HashMap::new();
        map.insert("int", Value::int(42));
        map.insert("float", Value::float(3.314));
        map.insert("bool", Value::bool(true));
        map.insert("text", Value::text("Hello"));
        map.insert("null", Value::null());
        map.insert("time", Value::time(3, 0));
        map.insert("date", Value::date(305));

        assert_eq!(map.get("int"), Some(&Value::int(42)));
        assert_eq!(map.get("float"), Some(&Value::float(3.314)));
        assert_eq!(map.get("bool"), Some(&Value::bool(true)));
        assert_eq!(map.get("text"), Some(&Value::text("Hello")));
        assert_eq!(map.get("null"), Some(&Value::null()));
        assert_eq!(map.get("time"), Some(&Value::time(3, 0)));
        assert_eq!(map.get("date"), Some(&Value::date(305)));
    }

    #[test]
    fn into() {
        let raws: Vec<Value> = vec![
            3.into(),
            5.into(),
            3.3.into(),
            "test".into(),
            false.into(),
            vec![3.into(), 7.into()].into(),
        ];
        let values = vec![
            Value::int(3),
            Value::int(5),
            Value::float(3.3),
            Value::text("test"),
            Value::bool(false),
            Value::array(vec![3.into(), 7.into()]),
        ];

        for (i, raw) in raws.iter().enumerate() {
            assert_eq!(raw, &values[i])
        }
    }

    #[test]
    fn hash() {
        let mut hasher = DefaultHasher::new();
        let a = Value::bool(true);
        a.hash(&mut hasher);
        let a = hasher.finish();
        let mut hasher = DefaultHasher::new();
        let b = Value::bool(true);
        b.hash(&mut hasher);
        let b = hasher.finish();

        assert_eq!(a, b);
    }

    #[test]
    fn test_add() {
        let value = add(1.into(), 2.into());
        assert_eq!(value, 3.into());
        assert_ne!(value, 0.into());

        let value = add(3.into(), 0.5.into());
        assert_eq!(value, 3.5.into());

        let value = add(3.into(), (-0.5).into());
        assert_eq!(value, 2.5.into());

        let value = add("test".into(), "test".into());
        assert_eq!(value, "testtest".into());

        let value = add("test".into(), true.into());
        assert_eq!(value, "testtrue".into());

        let value = add("test".into(), 1.5.into());
        assert_eq!(value, "test1.5".into());

        let value = add(Value::date(305), Value::date(5));
        assert_eq!(value, Value::date(310));

        let value = add(Value::date(305), 5.into());
        assert_eq!(value, Value::date(310));

        let value = add(Value::time(305, 5), 5.into());
        assert_eq!(value, Value::time(310, 5));

        let value = add(Value::time(305, 500000), 5.5.into());
        assert_eq!(value, Value::time(311, 0));

        let value = add(vec![1.into(), 2.into()].into(), 3.into());
        assert_eq!(value, vec![1.into(), 2.into(), 3.into()].into());
    }

    #[test]
    fn test_sub() {
        let value = sub(1.into(), 2.into());
        assert_eq!(value, (-1).into());
        assert_ne!(value, 0.into());

        let value = sub(3.into(), 0.5.into());
        assert_eq!(value, 2.5.into());

        let value = sub(3.into(), (-0.5).into());
        assert_eq!(value, 3.5.into());
    }

    #[test]
    fn test_mul() {
        let value = mul(2.into(), 2.into());
        assert_eq!(value, 4.into());
        assert_ne!(value, 0.into());

        let value = mul(2.into(), 1.5.into());
        assert_eq!(value, 3.into());

        let value = mul("test".into(), 3.into());
        assert_eq!(value, "testtesttest".into());
    }

    #[test]
    fn test_div() {
        let value = div(2.into(), 2.into());
        assert_eq!(value, 1.into());
        assert_ne!(value, 0.into());

        let value = div(2.into(), 4.into());
        assert_eq!(value, 0.5.into());

        let value = div(2.5.into(), 5.into());
        assert_eq!(value, 0.5.into());
    }

    fn add(a: Value, b: Value) -> Value {
        &a + &b
    }

    fn sub(a: Value, b: Value) -> Value {
        &a - &b
    }

    fn mul(a: Value, b: Value) -> Value {
        &a * &b
    }

    fn div(a: Value, b: Value) -> Value {
        &a / &b
    }
}
