use std::collections::BTreeMap;
use std::hash::Hash;
use std::ops::Add;
use crate::value::r#type::ValType;
use crate::value::Value;
use crate::value::value::Valuable;

#[derive(Eq, Clone, Debug, Hash, PartialEq, Default)]
pub struct Dict(pub BTreeMap<String, Value>);

impl Dict {
    pub fn new(values: BTreeMap<String, Value>) -> Self{
        Dict(values.into())
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
