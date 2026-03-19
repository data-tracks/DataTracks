use crate::{Dict, Int, Text};
use core::fmt::Display;
use serde::{Deserialize, Serialize};
use speedy::Readable;
use speedy::Writable;
use std::fmt::Formatter;

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
    pub id: Int,
    pub labels: Vec<Text>,
    pub properties: Dict,
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.id)?;
        write!(f, "{:?}", self.labels)?;
        write!(f, "{:?}", self.properties)
    }
}
