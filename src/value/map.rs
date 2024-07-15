use std::collections::HashMap;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};

use crate::value::Value;

#[derive(Eq, Clone, Debug)]
pub struct HoMap(HashMap<Value, Value>);


impl HoMap {
    pub fn new(map: HashMap<Value, Value>) -> Self {
        HoMap(map)
    }
}

impl Hash for HoMap {
    fn hash<H: Hasher>(&self, _: &mut H) {
        panic!()
    }
}

impl PartialEq for HoMap {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl std::fmt::Display for HoMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}