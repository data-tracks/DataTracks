use crate::value::Value;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// wagon holds context information
#[derive(Clone, Debug, Eq)]
pub struct Wagon {
    topic: Vec<String>,
    pub(crate) origin: usize,
    pub value: Box<Value>,
}

impl Wagon {
    pub fn new(value: Value, origin: usize) -> Wagon {
        Wagon { value: Box::new(value), topic: Vec::new(), origin }
    }

    pub(crate) fn unwrap(self) -> Value {
        *self.value
    }
}

impl PartialEq for Wagon {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Serialize for Wagon {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Wagon {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Wagon::new(Value::deserialize(deserializer)?, 0))
    }
}
