use crate::algebra::{BoxedIterator, ValueIterator};
use crate::processing::transform::Transform;
use crate::util::storage::ValueStore;
use std::collections::HashMap;
use value::Value;

pub struct EmptyIterator {}

impl Iterator for EmptyIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl ValueIterator for EmptyIterator {
    fn get_storage(&self) -> Vec<ValueStore> {
        vec![]
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(EmptyIterator {})
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}
