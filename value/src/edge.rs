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
pub struct Edge {
    pub label: Option<String>,
    pub start: usize,
    pub end: usize,
    pub properties: BTreeMap<Value, Value>,
}
