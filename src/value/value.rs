use crate::processing;
use crate::value::array::Array;
use crate::value::dict::Dict;
use crate::value::r#type::ValType;
use crate::value::string::Text;
use crate::value::Value::Wagon;
use crate::value::{bool, Bool, Float, Int};
use bytes::{BufMut, BytesMut};
use json::{parse, JsonValue};
use postgres::types::{IsNull, Type};
use rusqlite::types::{FromSqlResult, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Add, AddAssign, Div, Mul, Sub};
use tracing::warn;

#[derive(Eq, Clone, Debug, Serialize, Deserialize, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub enum Value {
    Int(Int),
    Float(Float),
    Bool(Bool),
    Text(Text),
    Array(Array),
    Dict(Dict),
    Null,
    Wagon(processing::Wagon),
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

    pub fn array(tuple: Vec<Value>) -> Value {
        Value::Array(Array::new(tuple))
    }

    pub(crate) fn dict(values: BTreeMap<String, Value>) -> Value {
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

    pub fn wagon(value: Value, origin: String) -> Value {
        Wagon(processing::Wagon::new(value, origin))
    }

    pub(crate) fn dict_from_kv(key: &str, value: Value) -> Value {
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
            Wagon(w) => w.value.type_()
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
            Value::Null => Err(()),
            Wagon(w) => w.value.as_int()
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
            Value::Null => Err(()),
            Wagon(w) => w.value.as_float()
        }
    }

    pub fn as_dict(&self) -> Result<Dict, ()> {
        match self {
            Value::Int(_) | Value::Float(_) | Value::Bool(_) | Value::Text(_) | Value::Array(_) | Value::Null => Err(()),
            Value::Dict(d) => Ok(d.clone()),
            Wagon(w) => w.value.as_dict()
        }
    }

    pub fn as_array(&self) -> Result<Array, ()> {
        match self {
            Value::Int(_) | Value::Float(_) | Value::Bool(_) | Value::Text(_) | Value::Dict(_) | Value::Null => Err(()),
            Value::Array(a) => Ok(a.clone()),
            Wagon(w) => w.value.as_array()
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
            Value::Array(a) => Ok(Bool(!a.values.is_empty())),
            Value::Dict(d) => Ok(Bool(!d.is_empty())),
            Value::Null => Ok(Bool(false)),
            Wagon(w) => w.value.as_bool()
        }
    }

    pub fn as_text(&self) -> Result<Text, ()> {
        match self {
            Value::Int(i) => Ok(Text(i.0.to_string())),
            Value::Float(f) => Ok(Text(f.as_f64().to_string())),
            Value::Bool(b) => Ok(Text(b.0.to_string())),
            Value::Text(t) => Ok(t.clone()),
            Value::Array(a) => Ok(Text(format!("[{}]", a.values.iter().map(|v| v.as_text().unwrap().0).collect::<Vec<String>>().join(",")))),
            Value::Dict(d) => Ok(Text(format!("[{}]", d.iter().map(|(k, v)| format!("{}:{}", k, v.as_text().unwrap().0)).collect::<Vec<String>>().join(",")))),
            Value::Null => Ok(Text("null".to_owned())),
            Wagon(w) => w.value.as_text()
        }
    }
}

fn flatten(dict: Dict, prefix: Vec<String>) -> Vec<(String, Value)> {
    let mut values = vec![];
    dict.into_iter().for_each(|(k, v)| {
        match v {
            Value::Dict(d) => {
                let mut prefix = prefix.clone();
                prefix.push(k);
                values.append(&mut flatten(d, prefix))
            }
            _ => values.push((k, v)),
        }
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


impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        warn!("{} == {}", self, other);

        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Wagon(w), o) => *o == *w.value,
            (Value::Null, _) | (_, Value::Null) => false,
            (Value::Int(_), Value::Float(_)) => self.as_float().map(|v| &Value::Float(v) == other).unwrap_or(false),
            (Value::Int(i), _) => other.as_int().map(|v| i.0 == v.0).unwrap_or(false),
            (Value::Float(f), Value::Float(other_f)) => {
                let a = f.normalize();
                let b = other_f.normalize();
                a.number == b.number && a.shift == b.shift
            }
            (Value::Float(_), _) => other.as_float().map(|v| self == &Value::Float(v)).unwrap_or(false),
            (Value::Bool(b), _) => other.as_bool().map(|other| other.0 == b.0).unwrap_or(false),

            (Value::Text(t), _) => other.as_text().map(|other| other.0 == t.0).unwrap_or(false),

            (Value::Array(a), _) => other.as_array().map(|other| {
                a.values.len() == other.values.len() && a.values.iter().zip(other.values.iter()).all(|(a, b)| a == b)
            }).unwrap_or(false),

            (Value::Dict(d), _) => other.as_dict().map(|other| {
                d.len() == other.len() && d.keys().eq(other.keys()) && d.values().eq(other.values())
            }).unwrap_or(false),
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
            Value::Null => {
                "null".hash(state);
            }
            Wagon(w) => {
                w.value.hash(state)
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
            Value::Null => write!(f, "null"),
            Wagon(w) => w.value.fmt(f)
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
            Value::Bool(b) => {
                b.0 = b.0 && rhs.as_bool().unwrap().0
            }
            Value::Text(t) => {
                t.0 += &rhs.as_text().unwrap().0
            }
            Value::Array(a) => {
                a.values.push(rhs)
            }
            Value::Dict(d) => {
                d.append(&mut rhs.as_dict().unwrap())
            }
            Value::Null => {}
            Wagon(w) => w.value.add_assign(rhs),
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
            },
            (Value::Float(_), Value::Int(b)) => {
                Value::int(-b.0).add(self)
            }
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
            (Value::Text(text), Value::Int(b)) => {
                Value::text(&text.0.repeat(b.0 as usize))
            }
            (lhs, rhs) => panic!("Cannot multiply {:?} with {:?}.", lhs, rhs),
        }
    }
}

impl Div for &Value {
    type Output = Value;

    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Int(a), Value::Int(b)) => {
                Value::float(a.0 as f64 / b.0 as f64)
            }
            (Value::Int(a), Value::Float(b)) => {
                Value::float(a.0 as f64 / b.as_f64())
            }
            (Value::Float(a), Value::Int(b)) => {
                Value::float(a.as_f64() / b.0 as f64)
            }
            (Value::Float(a), Value::Float(b)) => {
                Value::float(a.as_f64() / b.as_f64())
            }
            (Wagon(w), b) => w.value.div(b),
            _ => panic!("Cannot divide {:?} with {:?}.", self, rhs)
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
        }else {
            Value::array(values)
        }
    }
}

impl<'a> postgres::types::FromSql<'a> for Value {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match *ty {
            Type::BOOL => Ok(Value::bool(postgres::types::FromSql::from_sql(ty, raw)?)),
            Type::TEXT | Type::CHAR => Ok(Value::text(postgres::types::FromSql::from_sql(ty, raw)?)),
            Type::INT2 | Type::INT4 | Type::INT8 => Ok(Value::int(postgres::types::FromSql::from_sql(ty, raw)?)),
            Type::FLOAT4 | Type::FLOAT8 => Ok(Value::float(postgres::types::FromSql::from_sql(ty, raw)?)),
            _ => Err(format!("Unrecognized value type: {}", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::TEXT | Type::CHAR | Type::BOOL | Type::INT2 | Type::INT8 | Type::INT4 | Type::FLOAT4 | Type::FLOAT8)
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
        Self: Sized
    {
        match self {
            Value::Int(i) => out.put_i64(i.0),
            Value::Float(f) => out.put_f64(f.as_f64()),
            Value::Bool(b) => out.extend_from_slice(&[b.0 as u8]),
            Value::Text(t) => out.extend_from_slice(t.0.as_bytes()),
            Value::Array(_) => return Err("Array not supported".into()),
            Value::Dict(_) => return Err("Dict not supported".into()),
            Value::Null => return Ok(IsNull::Yes),
            Wagon(w) => return w.clone().unwrap().to_sql(_ty, out),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized
    {
        matches!(*ty, Type::TEXT | Type::BOOL | Type::INT8 | Type::INT4 | Type::INT2 | Type::FLOAT4 | Type::FLOAT8)
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
            Value::Array(_) => Err(rusqlite::Error::InvalidQuery),
            Value::Dict(_) => Err(rusqlite::Error::InvalidQuery),
            Value::Null => Ok(ToSqlOutput::from(rusqlite::types::Null)),
            Wagon(w) => {
                w.value.to_sql()
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::value::Value;
    use std::collections::hash_map::DefaultHasher;
    use std::collections::HashMap;
    use std::hash::{Hash, Hasher};

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