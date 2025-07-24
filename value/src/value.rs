use crate::array::Array;
use crate::date::Date;
use crate::dict::Dict;
use crate::r#type::ValType;
use crate::text::Text;
use crate::time::Time;
use crate::value::Value::Null;
use crate::Value::Wagon;
use crate::{bool, wagon, Bool, Float, Int};
use bytes::{BufMut, BytesMut};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use json::JsonValue;
use postgres::types::{IsNull, Type};
use redb::{Key, TypeName};
use rumqttc::{Event, Incoming};
use rumqttd::protocol::Publish;
use rumqttd::Notification;
use rusqlite::types::{FromSqlResult, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::cmp::{Ordering, PartialEq};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Add, AddAssign, Div, Mul, Sub};
use std::str;
use tracing::debug;
use track_rails::message_generated::protocol::{
    Null as FlatNull, NullArgs, Value as FlatValue, ValueWrapper, ValueWrapperArgs,
};

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Readable, Writable)]
pub enum Value {
    Int(Int),
    Float(Float),
    Bool(Bool),
    Text(Text),
    Time(Time),
    Date(Date),
    Array(Array),
    Dict(Dict),
    Null,
    Wagon(wagon::Wagon),
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

    pub fn array(tuple: Vec<Value>) -> Value {
        Value::Array(Array::new(tuple))
    }

    pub fn flatternize<'bldr>(
        &self,
        builder: &mut FlatBufferBuilder<'bldr>,
    ) -> WIPOffset<ValueWrapper<'bldr>> {
        let data_type = self.get_flat_type();

        let data = match self {
            Value::Int(i) => Some(i.flatternize(builder).as_union_value()),
            Value::Float(i) => Some(i.flatternize(builder).as_union_value()),
            Value::Bool(i) => Some(i.flatternize(builder).as_union_value()),
            Value::Text(i) => Some(i.flatternize(builder).as_union_value()),
            Value::Time(i) => Some(i.flatternize(builder).as_union_value()),
            Value::Date(_) => todo!("remove"),
            Value::Array(i) => Some(i.flatternize(builder).as_union_value()),
            Value::Dict(i) => Some(i.flatternize(builder).as_union_value()),
            Null => Some(FlatNull::create(builder, &NullArgs {}).as_union_value()),
            Wagon(i) => return i.value.flatternize(builder),
        };

        ValueWrapper::create(builder, &ValueWrapperArgs { data_type, data })
    }

    pub(crate) fn get_flat_type(&self) -> FlatValue {
        match self {
            Value::Int(_) => FlatValue::Integer,
            Value::Float(_) => FlatValue::Float,
            Value::Bool(_) => FlatValue::Bool,
            Value::Text(_) => FlatValue::Text,
            Value::Time(_) => FlatValue::Time,
            Value::Date(_) => todo!("remove"),
            Value::Array(_) => FlatValue::List,
            Value::Dict(_) => FlatValue::Document,
            Null => FlatValue::Null,
            Wagon(w) => w.value.get_flat_type(),
        }
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

    pub fn wagon(value: Value, origin: Value) -> Value {
        Wagon(wagon::Wagon::new(value, origin))
    }

    pub fn dict_from_kv(key: &str, value: Value) -> Value {
        Self::dict_from_pairs(vec![(key, value)])
    }

    pub fn dict_from_pairs(pairs: Vec<(&str, Value)>) -> Value {
        let mut map = BTreeMap::new();
        pairs.into_iter().for_each(|(k, v)| {
            map.insert(k.to_string(), v);
        });
        Value::Dict(Dict::new(map))
    }

    pub fn null() -> Value {
        Null
    }

    pub fn type_(&self) -> ValType {
        match self {
            Value::Int(_) => ValType::Integer,
            Value::Float(_) => ValType::Float,
            Value::Bool(_) => ValType::Bool,
            Value::Text(_) => ValType::Text,
            Value::Array(_) => ValType::Array,
            Value::Dict(_) => ValType::Dict,
            Null => ValType::Null,
            Wagon(w) => w.value.type_(),
            Value::Time(_) => ValType::Time,
            Value::Date(_) => ValType::Date,
        }
    }

    pub fn as_int(&self) -> Result<Int, String> {
        match self {
            Value::Int(i) => Ok(*i),
            Value::Float(f) => Ok(Int(f.as_f64() as i64)),
            Value::Bool(b) => Ok(if b.0 { Int(1) } else { Int(0) }),
            Value::Text(t) => t.0.parse::<i64>().map(Int).map_err(|err| err.to_string()),
            Value::Array(_) => Err(String::from("Array cannot be converted")),
            Value::Dict(_) => Err(String::from("Dict cannot be converted")),
            Null => Err(String::from("Null cannot be converted")),
            Wagon(w) => w.value.as_int(),
            Value::Time(t) => Ok(Int(t.ms)),
            Value::Date(d) => Ok(Int::new(d.as_epoch())),
        }
    }

    pub fn as_float(&self) -> Result<Float, String> {
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
                    .map_err(|e| e.to_string())
            }
            Value::Array(_) => Err(String::from("Array cannot be converted")),
            Value::Dict(_) => Err(String::from("Dict cannot be converted")),
            Null => Err(String::from("Null cannot be converted")),
            Wagon(w) => w.value.as_float(),
            Value::Time(t) => Ok(if t.ns != 0 {
                Float::new(t.ms as f64 + t.ns as f64 / 1e6)
            } else {
                Float::new(t.ms as f64)
            }),
            Value::Date(d) => Ok(Float::new(d.as_epoch() as f64)),
        }
    }

    pub fn as_time(&self) -> Result<Time, String> {
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
            Value::Array(_) => Err(String::from("Array cannot be converted")),
            Value::Dict(_) => Err(String::from("Dict cannot be converted")),
            Null => Err(String::from("Null cannot be converted")),
            Wagon(w) => w.value.as_time(),
            Value::Date(_) => Ok(Time::new(0, 0)),
        }
    }

    pub fn as_date(&self) -> Result<Date, String> {
        match self {
            Value::Int(i) => Ok(Date::new(i.0)),
            Value::Float(f) => Ok(Date::new(f.as_f64() as i64)),
            Value::Bool(b) => Ok(Date::new(b.0 as i64)),
            Value::Text(t) => Ok(Date::from(t.0.clone())),
            Value::Time(_) => Err(String::from("Time cannot be converted")),
            Value::Date(d) => Ok(d.clone()),
            Value::Array(_) => Err(String::from("Array cannot be converted")),
            Value::Dict(_) => Err(String::from("Dict cannot be converted")),
            Null => Err(String::from("Null cannot be converted")),
            Wagon(w) => w.value.as_date(),
        }
    }

    pub fn as_dict(&self) -> Result<Dict, String> {
        match self {
            Value::Time(_)
            | Value::Int(_)
            | Value::Float(_)
            | Value::Bool(_)
            | Value::Text(_)
            | Value::Array(_)
            | Value::Date(_)
            | Null => Err(String::from("Dict cannot be converted")),
            Value::Dict(d) => Ok(d.clone()),
            Wagon(w) => w.value.as_dict(),
        }
    }

    pub fn as_array(&self) -> Result<Array, String> {
        match self {
            Value::Time(_)
            | Value::Int(_)
            | Value::Float(_)
            | Value::Bool(_)
            | Value::Text(_)
            | Value::Date(_)
            | Value::Dict(_)
            | Null => Err(String::from("Array cannot be converted")),
            Value::Array(a) => Ok(a.clone()),
            Wagon(w) => w.value.as_array(),
        }
    }
    pub fn as_bool(&self) -> Result<Bool, String> {
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
            Null => Ok(Bool(false)),
            Wagon(w) => w.value.as_bool(),
            Value::Date(d) => Ok(Bool::new(d.days > 0)),
        }
    }

    pub fn as_text(&self) -> Result<Text, String> {
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
            Null => Ok(Text("null".to_owned())),
            Wagon(w) => w.value.as_text(),
            Value::Time(t) => Ok(Text(t.to_string())),
            Value::Date(d) => Ok(Text(d.to_string())),
        }
    }

    pub fn wagonize(self, stop: usize) -> Value {
        match self {
            Wagon(mut w) => {
                w.origin = Box::new(stop.into());
                Wagon(w)
            }
            value => Value::wagon(value, stop.into()),
        }
    }
}

impl TryFrom<&Vector<'_, ForwardsUOffset<ValueWrapper<'_>>>> for Value {
    type Error = String;

    fn try_from(
        _value: &Vector<'_, ForwardsUOffset<ValueWrapper<'_>>>,
    ) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl TryFrom<ValueWrapper<'_>> for Value {
    type Error = String;

    fn try_from(value: ValueWrapper) -> Result<Self, Self::Error> {
        match value.data_type() {
            FlatValue::Time => {
                let time = value.data_as_time().ok_or("Could not find time")?;
                Ok(Value::time(time.data(), 0))
            }
            FlatValue::Text => {
                let string = value.data_as_text().ok_or("Could not find string")?;
                let data: &str = string.data();
                let string = data.into();
                Ok(string)
            }
            FlatValue::Float => {
                let float = value.data_as_float().ok_or("Could not find float")?;
                Ok(Value::float(float.data() as _))
            }
            FlatValue::Null => Ok(Value::null()),
            FlatValue::Integer => {
                let integer = value.data_as_integer().ok_or("Could not find integer")?;
                Ok(Value::int(integer.data()))
            }
            FlatValue::List => {
                let list = value.data_as_list().ok_or("Could not find list")?;
                let list = list.data();
                Ok(Value::array(
                    list.iter()
                        .map(|v| v.try_into())
                        .collect::<Result<_, _>>()?,
                ))
            }
            FlatValue::Document => {
                todo!()
            }
            t => Err(format!("Unsupported type {:?}", t)),
        }
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
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
            (Null, Null) => true,
            (Wagon(w), o) => *o == *w.value,
            (Null, _) | (_, Null) => false,
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
                for (k, v) in d.clone() {
                    k.hash(state);
                    v.hash(state);
                }
            }
            Null => {
                "null".hash(state);
            }
            Wagon(w) => w.value.hash(state),
            Value::Time(t) => {
                t.ms.hash(state);
                t.ns.hash(state)
            }
            Value::Date(d) => {
                d.days.hash(state);
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
            Null => write!(f, "null"),
            Wagon(w) => w.value.fmt(f),
            Value::Date(d) => d.fmt(f),
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

impl From<&JsonValue> for Value {
    fn from(value: &JsonValue) -> Self {
        match value {
            JsonValue::Null => Value::null(),
            JsonValue::Short(a) => Value::text(a.as_str()),
            JsonValue::String(a) => Value::text(a),
            JsonValue::Number(a) => Value::int(a.as_fixed_point_i64(0).unwrap()),
            JsonValue::Boolean(a) => Value::bool(*a),
            JsonValue::Object(elements) => Value::dict(
                elements
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.into()))
                    .collect(),
            ),
            JsonValue::Array(elements) => Value::array(
                elements
                    .iter()
                    .map(|arg0: &JsonValue| arg0.into())
                    .collect(),
            ),
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Value::null(),
            serde_json::Value::Bool(b) => Value::bool(b),
            serde_json::Value::Number(n) => {
                if n.is_f64() {
                    Value::float(n.as_f64().unwrap())
                } else {
                    Value::int(n.as_i64().unwrap())
                }
            }
            serde_json::Value::String(s) => Value::text(&s),
            serde_json::Value::Array(a) => {
                let mut values = vec![];
                for value in a {
                    values.push(value.into());
                }
                Value::array(values)
            }
            serde_json::Value::Object(o) => o.into(),
        }
    }
}

impl From<serde_json::Map<String, serde_json::Value>> for Value {
    fn from(value: serde_json::Map<String, serde_json::Value>) -> Self {
        Value::Dict(value.into())
    }
}

impl From<serde_json::Map<String, serde_json::Value>> for Dict {
    fn from(value: serde_json::Map<String, serde_json::Value>) -> Self {
        let mut map = BTreeMap::new();
        for (key, value) in value {
            map.insert(key, value.into());
        }
        Dict::new(map)
    }
}

impl TryFrom<Notification> for Dict {
    type Error = String;

    fn try_from(value: Notification) -> Result<Self, Self::Error> {
        match value {
            Notification::Forward(f) => f.publish.try_into(),
            _ => Err(format!("Unexpected notification {:?}", value))?,
        }
    }
}

impl TryFrom<Publish> for Dict {
    type Error = String;

    fn try_from(publish: Publish) -> Result<Self, Self::Error> {
        let mut dict = BTreeMap::new();
        let value = str::from_utf8(&publish.payload)
            .map_err(|e| e.to_string())?
            .into();
        let topic = str::from_utf8(&publish.topic)
            .map_err(|e| e.to_string())?
            .into();
        dict.insert("$".to_string(), value);
        dict.insert("$topic".to_string(), topic);
        Ok(Value::dict(dict).into())
    }
}

impl redb::Value for Value {
    type SelfType<'a>
        = Value
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Value::read_from_buffer(data).expect("Failed to deserialize Value")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.write_to_vec().expect("Failed to serialize Value")
    }

    fn type_name() -> TypeName {
        TypeName::new("value")
    }
}

impl Key for Value {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let val1: Value = Value::read_from_buffer(data1).expect("Failed to deserialize Value");
        let val2: Value = Value::read_from_buffer(data2).expect("Failed to deserialize Value");
        val1.cmp(&val2)
    }
}

impl TryFrom<Event> for Dict {
    type Error = String;

    fn try_from(value: Event) -> Result<Self, Self::Error> {
        match value {
            Event::Incoming(i) => match i {
                Incoming::Publish(p) => {
                    let mut map = BTreeMap::new();
                    map.insert(
                        "$".to_string(),
                        Value::text(str::from_utf8(&p.payload).map_err(|e| e.to_string())?),
                    );
                    map.insert("$topic".to_string(), Value::text(&p.topic));
                    Ok(Value::dict(map).as_dict().unwrap())
                }
                _ => Err(format!("Unexpected Incoming publish {:?}", i))?,
            },
            Event::Outgoing(_) => Err(String::from("Unexpected Outgoing publish")),
        }
    }
}

impl Add for &Value {
    type Output = Value;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // Case where both are integers
            (Value::Int(a), Value::Int(b)) => Value::Int(*a + *b),

            // Mixing Integer and Float, ensure the result is a Float
            (Value::Int(a), Value::Float(b)) | (Value::Float(b), Value::Int(a)) => {
                Value::Float(*a + *b)
            }
            (Value::Float(a), Value::Float(b)) => Value::Float(*a + *b),
            // text
            (Value::Text(a), b) => {
                let b = b.as_text().unwrap();
                Value::text(&format!("{}{}", a.0, b.0))
            }
            // time
            (Value::Time(a), b) => {
                let ms = a.ms + b.as_time().unwrap().ms;
                let ns = a.ns + b.as_time().unwrap().ns;
                Value::time(ms, ns)
            }
            (Value::Date(a), b) => Value::date(a.days + b.as_date().unwrap().days),
            // array
            (Value::Array(a), b) => {
                let mut a = a.clone();
                a.values.push(b.clone());
                Value::Array(a)
            }
            // Handle Wagon custom addition
            (Wagon(w), rhs) => &*w.value.clone() + rhs,
            (lhs, Wagon(w)) => lhs + &*w.value.clone(),

            // Panic on unsupported types
            (lhs, rhs) => panic!("Cannot add {:?} with {:?}.", lhs, rhs),
        }
    }
}

impl AddAssign for Value {
    fn add_assign(&mut self, rhs: Self) {
        match self {
            Value::Int(i) => {
                i.0 += rhs.as_int().unwrap().0;
            }
            Value::Float(f) => {
                let rhs = rhs.as_float().unwrap();
                let diff = f.shift.abs_diff(rhs.shift);
                match (f, rhs) {
                    (l, r) if l.shift > r.shift => {
                        l.number += r.number * (10 ^ diff) as i64;
                    }
                    (l, r) if l.shift < r.shift => {
                        l.number = l.number * (10 ^ diff) as i64 + r.number;
                        l.shift = r.shift;
                    }
                    (l, r) => {
                        l.number += r.number;
                    }
                }
            }
            Value::Bool(b) => b.0 = b.0 && rhs.as_bool().unwrap().0,
            Value::Text(t) => t.0 += &rhs.as_text().unwrap().0,
            Value::Array(a) => a.values.push(rhs),
            Value::Dict(d) => d.append(&mut rhs.as_dict().unwrap()),
            Null => {}
            Wagon(w) => w.value.add_assign(rhs),
            Value::Time(t) => {
                let time = rhs.as_time().unwrap();
                t.ms += time.ms;
                t.ns += time.ns;
            }
            Value::Date(d) => {
                d.days += rhs.as_date().unwrap().days;
            }
        }
    }
}

impl Sub for &Value {
    type Output = Value;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Int(a), Value::Int(b)) => Value::int(a.0 - b.0),
            (Value::Int(_), Value::Float(b)) => {
                let right = Value::float_parts(-b.number, b.shift);
                right.add(self)
            }
            (Value::Float(_), Value::Int(b)) => Value::int(-b.0).add(self),
            (lhs, rhs) => panic!("Cannot subtract {:?} from {:?}.", lhs, rhs),
        }
    }
}

impl Mul for &Value {
    type Output = Value;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Int(a), Value::Int(b)) => Value::int(a.0 * b.0),
            (Value::Int(a), Value::Float(b)) | (Value::Float(b), Value::Int(a)) => {
                Value::float_parts(a.0 * b.number, b.shift)
            }
            (Value::Float(a), Value::Float(b)) => {
                let max = a.shift.max(b.shift);
                let shift_diff = a.shift.abs_diff(b.shift) as i64;
                Value::float_parts(a.number * b.number * (10 ^ shift_diff), max)
            }
            (Value::Text(text), Value::Int(b)) => Value::text(&text.0.repeat(b.0 as usize)),
            (lhs, rhs) => panic!("Cannot multiply {:?} with {:?}.", lhs, rhs),
        }
    }
}

impl Div for &Value {
    type Output = Value;

    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Int(a), Value::Int(b)) => Value::float(a.0 as f64 / b.0 as f64),
            (Value::Int(a), Value::Float(b)) => Value::float(a.0 as f64 / b.as_f64()),
            (Value::Float(a), Value::Int(b)) => Value::float(a.as_f64() / b.0 as f64),
            (Value::Float(a), Value::Float(b)) => Value::float(a.as_f64() / b.as_f64()),
            (Wagon(w), b) => w.value.div(b),
            _ => panic!("Cannot divide {:?} with {:?}.", self, rhs),
        }
    }
}

impl From<postgres::Row> for Value {
    fn from(row: postgres::Row) -> Self {
        let len = row.len();
        let mut values = Vec::with_capacity(len);
        for i in 0..len {
            values.push(row.get::<usize, Value>(i));
        }
        if values.len() == 1 {
            values.pop().unwrap()
        } else {
            Value::array(values)
        }
    }
}

impl<'a> postgres::types::FromSql<'a> for Value {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match *ty {
            Type::BOOL => Ok(Value::bool(postgres::types::FromSql::from_sql(ty, raw)?)),
            Type::TEXT | Type::CHAR => {
                Ok(Value::text(postgres::types::FromSql::from_sql(ty, raw)?))
            }
            Type::INT2 | Type::INT4 | Type::INT8 => {
                Ok(Value::int(postgres::types::FromSql::from_sql(ty, raw)?))
            }
            Type::FLOAT4 | Type::FLOAT8 => {
                Ok(Value::float(postgres::types::FromSql::from_sql(ty, raw)?))
            }
            _ => Err(format!("Unrecognized value type: {}", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::TEXT
                | Type::CHAR
                | Type::BOOL
                | Type::INT2
                | Type::INT8
                | Type::INT4
                | Type::FLOAT4
                | Type::FLOAT8
        )
    }
}

impl TryFrom<(&rusqlite::Row<'_>, usize)> for Value {
    type Error = rusqlite::Error;

    fn try_from(pair: (&rusqlite::Row<'_>, usize)) -> Result<Self, Self::Error> {
        let row = pair.0;
        let mut values = Vec::with_capacity(pair.1);
        for i in 0..pair.1 {
            let value_ref = row.get_ref(i)?;
            values.push(rusqlite::types::FromSql::column_result(value_ref)?);
        }
        if values.len() == 1 {
            Ok(values.pop().unwrap())
        } else {
            Ok(Value::array(values))
        }
    }
}

impl postgres::types::ToSql for Value {
    fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            Value::Int(i) => out.put_i64(i.0),
            Value::Float(f) => out.put_f64(f.as_f64()),
            Value::Bool(b) => out.extend_from_slice(&[b.0 as u8]),
            Value::Text(t) => out.extend_from_slice(t.0.as_bytes()),
            Value::Array(_) => return Err("Array not supported".into()),
            Value::Dict(_) => return Err("Dict not supported".into()),
            Null => return Ok(IsNull::Yes),
            Value::Time(t) => out.put_i128(t.ms as i128),
            Wagon(w) => return w.clone().unwrap().to_sql(_ty, out),
            Value::Date(d) => out.put_i64(d.days),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        matches!(
            *ty,
            Type::TEXT
                | Type::BOOL
                | Type::INT8
                | Type::INT4
                | Type::INT2
                | Type::FLOAT4
                | Type::FLOAT8
        )
    }

    postgres::types::to_sql_checked!();
}

impl rusqlite::types::FromSql for Value {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.data_type() {
            rusqlite::types::Type::Null => Ok(Value::null()),
            rusqlite::types::Type::Integer => Ok(Value::int(value.as_i64()?)),
            rusqlite::types::Type::Real => Ok(Value::float(value.as_f64()?)),
            rusqlite::types::Type::Text => Ok(Value::text(value.as_str()?)),
            rusqlite::types::Type::Blob => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

impl rusqlite::types::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Value::Int(i) => Ok(ToSqlOutput::from(i.0)),
            Value::Float(f) => Ok(ToSqlOutput::from(f.as_f64())),
            Value::Bool(b) => Ok(ToSqlOutput::from(b.0)),
            Value::Text(t) => Ok(ToSqlOutput::from(t.0.clone())),
            Value::Time(t) => Ok(ToSqlOutput::from(t.ms)),
            Value::Array(_) => Err(rusqlite::Error::InvalidQuery),
            Value::Dict(_) => Err(rusqlite::Error::InvalidQuery),
            Null => Ok(ToSqlOutput::from(rusqlite::types::Null)),
            Wagon(w) => w.value.to_sql(),
            Value::Date(d) => Ok(ToSqlOutput::from(d.days)),
        }
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
