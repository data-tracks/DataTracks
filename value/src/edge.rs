use crate::{Dict, Int, Text};
use core::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use speedy::Readable;
use speedy::Writable;

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
    pub properties: Dict,
}

impl Display for Edge {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}:{}-{}", self.id, self.start, self.end)?;
        write!(f, "{:?}", self.label)?;
        write!(f, "{:?}", self.properties)
    }
}
