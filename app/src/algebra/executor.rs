use crate::algebra::{BoxedIterator, ValueIterator};
use crate::processing::transform::Transform;
use crate::processing::Sender;
use crate::util::storage::ValueStore;
use crate::util::Tx;
use std::collections::{HashMap, VecDeque};
use value::train::Train;
use value::Value;

pub struct Executor {
    iterator: BoxedIterator,
    sender: Sender,
    storage: ValueStore,
    stop: usize,
}

impl Executor {
    pub fn new(stop: usize, mut iterator: BoxedIterator, sender: Sender) -> Self {
        let storage = ValueStore::new();
        iterator.set_storage(storage.clone());
        Executor {
            iterator,
            sender,
            storage,
            stop,
        }
    }

    pub(crate) fn attach(&mut self, num: usize, train_observer: Tx<Train>) {
        self.sender.add(num, train_observer);
    }

    pub(crate) fn detach(&mut self, num: usize) {
        self.sender.remove(num);
    }

    pub fn execute(&mut self, train: Train) {
        let train = train.mark(self.stop);

        let marks = train.marks.clone();
        let event_time = train.event_time.clone();

        match train.values {
            None => {}
            Some(values) => {
                self.storage.append(values);
            }
        }
        let mut train = self.iterator.drain_to_train(self.stop);
        train.event_time = event_time;
        train.marks = marks;

        self.sender.send(train.flag(self.stop));
    }
}

#[derive(Clone)]
pub struct IdentityIterator {
    values: VecDeque<Value>,
    storage: Option<ValueStore>,
}

impl IdentityIterator {
    pub fn new() -> Self {
        IdentityIterator {
            values: Default::default(),
            storage: None,
        }
    }

    fn load(&mut self) {
        match self.storage.as_mut() {
            None => {}
            Some(s) => {
                s.drain().into_iter().for_each(|v| {
                    self.values.push_back(v);
                });
            }
        }
    }
}

impl Iterator for IdentityIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() {
            self.load();
            if self.values.is_empty() {
                return None;
            }
        }
        self.values.pop_front()
    }
}

impl ValueIterator for IdentityIterator {
    fn set_storage(&mut self, storage: ValueStore) {
        self.storage = Some(storage);
    }

    fn drain_to_train(&mut self, stop: usize) -> Train {
        self.load();
        Train::new(self.values.drain(..).collect())
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(IdentityIterator {
            values: self.values.clone(),
            storage: self.storage.clone(),
        })
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}
