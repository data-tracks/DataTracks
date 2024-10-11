use crate::value::Value;
use json::parse;
use serde::{Deserialize, Serialize};
use std::collections::btree_map::{IntoIter, Keys, Values};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;

#[derive(Eq, Clone, Debug, Hash, PartialEq, Default, Serialize, Deserialize)]
pub struct Dict(BTreeMap<String, Value>);


impl Dict {
    pub fn new(values: BTreeMap<String, Value>) -> Self{
        Dict(values)
    }

    pub(crate) fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    pub(crate) fn get_data(&self) -> Option<&Value> {
        self.0.get("$")
    }

    pub(crate) fn insert(&mut self, key: String, value: Value) {
        self.0.insert(key, value);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn append(&mut self, other: &mut Dict) {
        self.0.append(&mut other.0);
    }

    pub(crate) fn keys(&self) -> Keys<String, Value> {
        self.0.keys()
    }

    pub(crate) fn values(&self) -> Values<String, Value> {
        self.0.values()
    }

    pub(crate) fn iter(&self) -> std::collections::btree_map::Iter<'_, String, Value> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn pop_first(&mut self) -> Option<(String, Value)> {
        self.0.pop_first()
    }

    pub(crate) fn get_data_mut(&mut self) -> Option<&mut Value> {
        self.0.get_mut("$")
    }

    pub(crate) fn merge(&self, other: Dict) -> Dict {
        let mut map = BTreeMap::new();
        for (key, value) in self.clone() {
            map.insert(key, value);
        }
        for (key, value) in other.clone() {
            map.insert(key, value);
        }
        if self.0.contains_key("$") && other.0.contains_key("$") {
            map.insert("$".into(), vec![self.get_data().unwrap().clone(), other.get_data().unwrap().clone()].into());
        }
        Dict(map)
    }
}

impl IntoIterator for Dict {
    type Item = (String, Value);
    type IntoIter = IntoIter<String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}


impl Display for Dict {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{{}}}", self.0.iter().map(|(k, v)| format!("{}: {}", k, v)).collect::<Vec<String>>().join(", "))
    }
}

impl From<Value> for Dict{
    fn from(value: Value) -> Self {
        let mut map = BTreeMap::new();
        match value {
            Value::Dict(d) => {
                for (key, value) in d.0 {
                    map.insert(key, value);
                }
            }
            i => {
                map.insert("$".into(), i);
            }
        }
        Dict(map)
    }
}

impl From<Vec<Value>> for Dict{
    fn from(value: Vec<Value>) -> Self {
        let mut map = BTreeMap::new();
        map.insert("$".into(), value.into());
        Dict(map)
    }
}

impl Dict {
    pub(crate) fn from_json(value: &str) -> Self {
        let mut map = BTreeMap::new();
        for (key, value) in parse(value).unwrap().entries() {
            map.insert(key.into(), value.into());
        }
        Dict(map)
    }
}

#[cfg(test)]
impl Dict {
    pub(crate) fn transform(values: Vec<Value>) -> Vec<Value> {
        let mut dicts = vec![];
        for value in values {
            dicts.push(Value::Dict(value.into()));
        }
        dicts
    }

    pub(crate) fn transform_with_stop(stop: i32, values: Vec<Value>) -> Vec<Value> {
        let mut dicts = vec![];
        for value in values {
            dicts.push(Value::dict_from_kv(&format!("${}", stop), Value::Dict(value.into())));
        }
        dicts
    }
}
