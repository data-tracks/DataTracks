use crate::Value;
use std::collections::BTreeMap;

#[derive(
    Eq, Clone, Debug, Default, Serialize, Deserialize, Ord, PartialOrd, Readable, Writable,
, PartialEq)]
pub struct Edge {
    values: BTreeMap<String, Value>,
    start: usize,
    end: usize,
    properties: BTreeMap<Value, Value>,
}
