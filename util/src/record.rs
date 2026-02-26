use std::ops::{Deref, DerefMut};
use crate::{InitialMeta, TargetedMeta, TimedMeta};
use chrono::Utc;
use speedy::{Readable, Writable};
use std::slice::Iter;
use std::vec::IntoIter;
use value::Value;

pub trait Identifiable{
    fn id(&self) -> u64;
}

#[derive(Clone, Debug, Writable, Readable)]
pub struct Batch<T> {
    pub timestamp: i64,
    pub records: Vec<T>,
}

#[macro_export]
macro_rules! batch {
    ($($item:expr),*) => {
        $crate::Batch::new(vec![$($item),*])
    };
}

impl<T> Batch<T> {
    pub fn new(records: Vec<T>) -> Self {
        Self {
            timestamp: Utc::now().timestamp_millis(),
            records,
        }
    }

    pub fn push(&mut self, item: T) {
        self.records.push(item);
    }

    pub fn clear(&mut self) {
        self.records.clear();
    }

    pub fn pop(&mut self) -> Option<T> {
        self.records.pop()
    }
}

impl<T> Default for Batch<T> {
    fn default() -> Self {
        Batch::new(vec![])
    }
}

impl<T> FromIterator<T> for Batch<T>  {
    fn from_iter<I: IntoIterator<Item=T>>(iter: I) -> Self {
        let items = Vec::from_iter(iter);

        Batch::new(items)
    }
}

impl<T> Deref for Batch<T> {
    type Target = [T]; // We target a slice for maximum compatibility

    fn deref(&self) -> &Self::Target {
        &self.records
    }
}

impl<T> DerefMut for Batch<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.records
    }
}

impl<T> IntoIterator for Batch<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Batch<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}


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