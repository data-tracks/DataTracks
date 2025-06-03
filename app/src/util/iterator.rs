use std::collections::HashMap;
use crate::algebra::{BoxedIterator, ValueIterator};
use crate::processing::Train;
use crate::processing::transform::Transform;
use value::Value;
use crate::util::storage::Storage;

pub struct EmptyIterator {}

impl Iterator for EmptyIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl ValueIterator for EmptyIterator {
    fn set_storage(&mut self, storage: &Storage) {
        // nothing on purpose
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(EmptyIterator {})
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}