use std::ops::{Deref, DerefMut};
use std::slice::Iter;
use chrono::Utc;
use smallvec::alloc;
use speedy::{Readable, Writable};

#[derive(Clone, Debug, Writable, Readable)]
pub struct Batch<T> {
    pub timestamp: i64,
    pub records: Vec<T>,
}

#[macro_export]
macro_rules! batch {
    ($($item:expr),*) => {
        let mut b = $crate::Batch::with_capacity($crate::count_items!($($item),*));
        $(b.push($item);)*
        b
    };
}

impl<T> Batch<T> {
    pub fn new(records: Vec<T>) -> Self {
        Self {
            timestamp: Utc::now().timestamp_millis(),
            records
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            timestamp: Utc::now().timestamp_millis(),
            records: Vec::with_capacity(capacity),
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
    type IntoIter = alloc::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Batch<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}
