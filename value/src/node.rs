use std::collections::BTreeMap;
use crate::Value;

#[derive(
    Eq, Clone, Debug, Default, Serialize, Deserialize, Ord, PartialOrd, Readable, Writable,
, PartialEq)]
pub struct Node {
    labels: Vec<String>,
    properties: BTreeMap<Value, Value>,
}
