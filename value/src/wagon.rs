use crate::value::Value;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};

// wagon holds context information
#[derive(Clone, Debug, Eq, Ord, PartialOrd, Readable, Writable, Serialize, Deserialize)]
pub struct Wagon {
    pub topic: Vec<String>,
    pub origin: Box<Value>,
    pub value: Box<Value>,
}

impl Wagon {
    pub fn new(value: Value, origin: Value) -> Wagon {
        Wagon {
            value: Box::new(value),
            topic: Vec::new(),
            origin: Box::new(origin),
        }
    }

    pub fn unwrap(self) -> Value {
        *self.value
    }
}

impl PartialEq for Wagon {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}


