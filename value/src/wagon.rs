use crate::train::Train;
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

impl From<Vec<Wagon>> for Train {
    fn from(value: Vec<Wagon>) -> Self {
        Train::new_values(
            value.into_iter().map(Value::Wagon).collect::<Vec<Value>>(),
            0,
            0,
        )
    }
}

impl From<Vec<Value>> for Train {
    fn from(value: Vec<Value>) -> Self {
        Train::new_values(
            value
                .into_iter()
                .map(|v| Value::wagon(v, Value::null()))
                .collect::<Vec<Value>>(),
            0,
            0,
        )
    }
}
