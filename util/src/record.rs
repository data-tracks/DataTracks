use crate::{Identifiable, InitialMeta, TargetedMeta, TimedMeta};
use speedy::{Readable, Writable};
use value::Value;

#[derive(Clone, Debug, Writable, Readable)]
pub enum Record {
    Initial(Box<InitialRecord>),
    Time(Box<TimedRecord>),
    Target(Box<TargetedRecord>),
}

#[derive(Clone, Debug, Writable, Readable)]
pub struct InitialRecord {
    pub value: Value,
    pub meta: InitialMeta,
}

impl From<(Value, InitialMeta)> for InitialRecord {
    fn from(value: (Value, InitialMeta)) -> Self {
        InitialRecord {
            value: value.0,
            meta: value.1,
        }
    }
}

#[derive(Clone, Debug, Writable, Readable, Eq, PartialEq)]
pub struct TimedRecord {
    pub value: Value,
    pub meta: TimedMeta,
}

impl TimedRecord {
    pub fn id(&self) -> u64 {
        self.meta.id
    }
}

impl Identifiable for TimedRecord {
    fn id(&self) -> u64 {
        self.id()
    }
}

impl From<(Value, TimedMeta)> for TimedRecord {
    fn from(value: (Value, TimedMeta)) -> Self {
        TimedRecord {
            value: value.0,
            meta: value.1,
        }
    }
}

#[derive(Clone, Debug, Writable, Readable)]
pub struct TargetedRecord {
    pub value: Value,
    pub meta: TargetedMeta,
}

impl TargetedRecord {
    pub fn id(&self) -> u64 {
        self.meta.id
    }
}

impl From<(Value, TargetedMeta)> for TargetedRecord {
    fn from(value: (Value, TargetedMeta)) -> Self {
        TargetedRecord {
            value: value.0,
            meta: value.1,
        }
    }
}

impl Identifiable for TargetedRecord {
    fn id(&self) -> u64 {
        self.id()
    }
}

/// Generate a TimedRecord from a value and TimedMeta
#[macro_export]
macro_rules! timed {
    ($val:expr, $meta:expr) => {
        $crate::TimedRecord {
            value: $val,
            meta: $meta,
        }
    };
}
/// Generate a TargetedRecord from a value and TargetedMeta
#[macro_export]
macro_rules! target {
    ($val:expr, $meta:expr) => {
        $crate::TargetedRecord {
            value: $val,
            meta: $meta
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_sizes() {
        println!("Value: {} bytes", size_of::<Value>());
        println!("Record: {} bytes", size_of::<Record>());
    }
}