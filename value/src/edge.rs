use serde::{Deserialize, Serialize};
use speedy::Readable;
use speedy::Writable;
use std::collections::BTreeMap;
use crate::{Int, Text};
use crate::value::Value;

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
    pub id: Int,
    pub label: Option<Text>,
    pub start: u64,
    pub end: u64,
    pub properties: BTreeMap<String, Value>,
}
