use crate::value::Value;
use json::parse;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::Zip;

#[derive(
    Eq, Clone, Debug, Default, Serialize, Deserialize, Ord, PartialOrd, Readable, Writable,
)]
pub struct Dict {
    pub keys: Vec<String>,
    pub values: Vec<Value>,
}

impl Dict {
    pub fn new(map: HashMap<String, Value>) -> Self {
        let (keys, values) = map.into_iter().unzip();

        Dict { keys, values }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.keys.iter().zip(self.values.iter())
    }

    pub fn get<S: AsRef<str>>(&self, key: S) -> Option<&Value> {
        let index = self.keys.iter().position(|k| k == key.as_ref())?;
        self.values.get(index)
    }

    pub fn insert(&mut self, key: String, value: Value) {
        self.keys.push(key);
        self.values.push(value)
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub(crate) fn append(&mut self, other: &mut Dict) {
        self.values.append(&mut other.values);
    }

    pub(crate) fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn values(&self) -> &Vec<Value> {
        &self.values
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }
}

impl IntoIterator for Dict {
    type Item = (String, Value);
    type IntoIter = Zip<std::vec::IntoIter<String>, std::vec::IntoIter<Value>>;

    fn into_iter(self) -> Self::IntoIter {
        self.keys.into_iter().zip(self.values)
    }
}

impl PartialEq for Dict {
    fn eq(&self, other: &Self) -> bool {
        if self.keys.len() != other.keys.len() {
            return false;
        }

        let mut pairs1: Vec<(&String, &Value)> = self.iter().collect();
        let mut pairs2: Vec<(&String, &Value)> = other.iter().collect();

        pairs1.sort_by_key(|&(k, _)| k);
        pairs2.sort_by_key(|&(k, _)| k);

        pairs1 == pairs2
    }
}

impl Hash for Dict {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.values.hash(state);
    }
}

impl From<(Vec<String>, Vec<Value>)> for Dict {
    fn from(value: (Vec<String>, Vec<Value>)) -> Self {
        let (keys, values) = value;
        Dict { keys, values }
    }
}

impl From<Vec<(&str, Value)>> for Dict {
    fn from(value: Vec<(&str, Value)>) -> Self {
        let (keys, values) = value.into_iter().map(|(k, v)| (k.to_string(), v)).unzip();

        Dict { keys, values }
    }
}

impl From<Vec<(String, Value)>> for Dict {
    fn from(value: Vec<(String, Value)>) -> Self {
        let (keys, values) = value.into_iter().unzip();
        Dict { keys, values }
    }
}

impl From<(&str, Value)> for Dict {
    fn from(value: (&str, Value)) -> Self {
        let (key, value) = value;
        Dict {
            keys: vec![key.to_string()],
            values: vec![value],
        }
    }
}

impl From<(String, Value)> for Dict {
    fn from(value: (String, Value)) -> Self {
        let (key, value) = value;
        Dict {
            keys: vec![key],
            values: vec![value],
        }
    }
}

impl From<HashMap<String, Value>> for Dict {
    fn from(map: HashMap<String, Value>) -> Self {
        let (keys, values) = map.into_iter().unzip();
        Dict { keys, values }
    }
}

impl From<(Vec<String>, Vec<Value>)> for Value {
    fn from(value: (Vec<String>, Vec<Value>)) -> Self {
        Value::Dict(Box::new(value.into()))
    }
}

impl Display for Dict {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{}}}",
            self.keys
                .iter()
                .zip(self.values.iter())
                .map(|(k, v)| format!("{k}: {v}"))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl From<Value> for Dict {
    fn from(value: Value) -> Self {
        match value {
            Value::Dict(d) => *d,
            i => Dict {
                keys: vec!["$".to_string()],
                values: vec![i],
            },
        }
    }
}

impl From<Vec<Value>> for Dict {
    fn from(value: Vec<Value>) -> Self {
        Dict {
            keys: vec!["$".to_string()],
            values: value,
        }
    }
}

impl Dict {
    pub fn from_json(value: &str) -> Self {
        let (keys, values) = parse(value)
            .unwrap()
            .entries()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.into()))
            .unzip();

        Dict { keys, values }
    }
}
