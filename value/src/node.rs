use crate::Value;
use serde::{Deserialize, Serialize};
use speedy::Readable;
use speedy::Writable;
use std::collections::BTreeMap;

#[derive(
    Eq,
    Clone,
    Debug,
    Default,
    Serialize,
    Deserialize,
    Ord,
    PartialOrd,
    Readable,
    Writable,
    PartialEq,
)]
pub struct Node {
    pub id: Value,
    pub labels: Vec<Value>,
    pub properties: BTreeMap<String, Value>,
}
