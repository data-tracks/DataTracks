use std::collections::{BTreeMap, VecDeque};

#[derive(Clone)]
pub struct Cache<Key, Element> {
    values: BTreeMap<Key, Element>,
    order: VecDeque<Key>,
    max: usize,
}

impl<Key: Ord, Element> Cache<Key, Element> {
    pub fn new(max: usize) -> Cache<Key, Element> {
        Cache{values: BTreeMap::new(), order: VecDeque::new(), max}
    }

    pub fn get(&self, key: &Key) -> Option<&Element> {
        self.values.get(key)
    }

    pub fn put(&mut self, key: Key, element: Element) {
        self.values.insert(key, element);
        if self.order.len() > self.max {
            self.order.pop_front();
        }
    }
}