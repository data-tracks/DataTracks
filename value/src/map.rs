use std::collections::HashMap;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};

use crate::value::Value;

#[derive(Eq, Clone, Debug)]
pub struct HoMap(HashMap<Value, Value>);

impl HoMap {
    pub fn _new(map: HashMap<Value, Value>) -> Self {
        HoMap(map)
    }
}

impl Hash for HoMap {
    fn hash<H: Hasher>(&self, hash: &mut H) {
        hash.write_u64(self.0.len() as u64);
        for (k, v) in self.0.iter() {
            k.hash(hash);
            v.hash(hash);
        }
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
