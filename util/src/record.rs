use crate::{InitialMeta, TargetedMeta, TimedMeta};
use speedy::{Readable, Writable};
use value::Value;

#[derive(Clone, Debug, Writable, Readable)]
pub enum Record {
    Initial(InitialRecord),
    Time(TimedRecord),
    Target(TargetedRecord),
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

#[derive(Clone, Debug, Writable, Readable)]
pub struct TimedRecord {
    pub value: Value,
    pub meta: TimedMeta,
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

impl From<(Value, TargetedMeta)> for TargetedRecord {
    fn from(value: (Value, TargetedMeta)) -> Self {
        TargetedRecord {
            value: value.0,
            meta: value.1,
        }
    }
}
