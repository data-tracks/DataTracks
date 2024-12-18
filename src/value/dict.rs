use crate::value::Value;
use json::parse;
use serde::{Deserialize, Serialize};
use std::collections::btree_map::{IntoIter, Keys, Values};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use rkyv::Place;

#[derive(Eq, Clone, Debug, Default, Serialize, Deserialize, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct Dict {
    values: BTreeMap<String, Value>,
    alternative: BTreeMap<String, String>, // "alternative_name" -> Value
}


impl Dict {
    pub fn new(values: BTreeMap<String, Value>) -> Self{
        Dict { values, alternative: BTreeMap::new() }
    }

    pub fn prefix_all(&mut self, prefix: &str) {
        self.values.iter().for_each(|(name, _field)| {
            self.alternative.insert(format!("{}{}", prefix, name), name.clone());
        });
    }

    pub(crate) fn get(&self, key: &str) -> Option<&Value> {
        match self.values.get(key) {
            None => match self.alternative.get(key) {
                None => None,
                Some(s) => self.values.get(s)
            }
            Some(s) => Some(s)
        }
    }

    pub(crate) fn get_data(&self) -> Option<&Value> {
        self.values.get("$")
    }

    pub(crate) fn insert(&mut self, key: String, value: Value) {
        self.values.insert(key, value);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub(crate) fn append(&mut self, other: &mut Dict) {
        self.values.append(&mut other.values);
    }

    pub(crate) fn keys(&self) -> Keys<String, Value> {
        self.values.keys()
    }

    pub(crate) fn values(&self) -> Values<String, Value> {
        self.values.values()
    }

    pub(crate) fn iter(&self) -> std::collections::btree_map::Iter<'_, String, Value> {
        self.values.iter()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn pop_first(&mut self) -> Option<(String, Value)> {
        self.values.pop_first()
    }

    pub(crate) fn get_data_mut(&mut self) -> Option<&mut Value> {
        self.values.get_mut("$")
    }

    pub(crate) fn merge(&self, other: Dict) -> Dict {
        let mut map = BTreeMap::new();
        for (key, value) in self.clone() {
            map.insert(key, value);
        }
        for (key, value) in other.clone() {
            map.insert(key, value);
        }
        if self.values.contains_key("$") && other.values.contains_key("$") {
            map.insert("$".into(), vec![self.get_data().unwrap().clone(), other.get_data().unwrap().clone()].into());
        }
        Dict { values: map, alternative: Default::default() }
    }
}

impl PartialEq for Dict {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl Hash for Dict {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.values.hash(state);
    }
}

impl IntoIterator for Dict {
    type Item = (String, Value);
    type IntoIter = IntoIter<String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}


impl Display for Dict {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{{}}}", self.values.iter().map(|(k, v)| format!("{}: {}", k, v)).collect::<Vec<String>>().join(", "))
    }
}

impl From<Value> for Dict{
    fn from(value: Value) -> Self {
        let mut map = BTreeMap::new();
        match value {
            Value::Dict(d) => {
                for (key, value) in d.values {
                    map.insert(key, value);
                }
            }
            i => {
                map.insert("$".into(), i);
            }
        }
        Dict { values: map, alternative: BTreeMap::new() }
    }
}

impl From<Vec<Value>> for Dict{
    fn from(value: Vec<Value>) -> Self {
        let mut map = BTreeMap::new();
        map.insert("$".into(), value.into());
        Dict { values: map, alternative: BTreeMap::new() }
    }
}

impl Dict {
    pub(crate) fn from_json(value: &str) -> Self {
        let mut map = BTreeMap::new();
        for (key, value) in parse(value).unwrap().entries() {
            map.insert(key.into(), value.into());
        }
        Dict { values: map, alternative: BTreeMap::new() }
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
}
