use std::collections::btree_map::IntoIter;
use std::collections::BTreeMap;
use std::hash::Hash;
use json::parse;
use crate::value::r#type::ValType;
use crate::value::Value;
use crate::value::value::Valuable;

#[derive(Eq, Clone, Debug, Hash, PartialEq, Default)]
pub struct Dict(pub BTreeMap<String, Value>);

impl Dict {
    pub fn new(values: BTreeMap<String, Value>) -> Self{
        Dict(values.into())
    }

    pub(crate) fn get(&self, key: &String) -> Option<&Value> {
        self.0.get(key)
    }

    pub(crate) fn get_data(&self) -> Option<&Value> {
        self.0.get("$")
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

impl Valuable for Dict {
    fn type_(&self) -> ValType {
        ValType::Dict
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

impl IntoIterator for Dict {
    type Item = (String, Value);
    type IntoIter = IntoIter<String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}


impl Dict {
    pub(crate) fn transform(values: Vec<Value>) -> Vec<Dict> {
        let mut dicts = vec![];
        for value in values {
            dicts.push(value.into());
        }
        dicts
    }
}
