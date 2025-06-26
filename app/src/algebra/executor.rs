use crate::algebra::{BoxedIterator, ValueIterator};
use crate::processing::transform::Transform;
use crate::processing::Sender;
use crate::util::storage::ValueStore;
use crate::util::Tx;
use rusqlite::Map;
use std::collections::{HashMap, VecDeque};
use tracing::warn;
use value::train::Train;
use value::Value;

enum WhatStrategy {
    Task(BoxedIterator, HashMap<usize, ValueStore>),
    Direct,
}

/// responsible for handling where and what of data points and hands them over to the next component
pub struct Executor {
    what: WhatStrategy,
    sender: Sender,
    stop: usize,
}

impl Executor {
    pub fn new(stop: usize, mut iterator: Option<BoxedIterator>, sender: Sender) -> Self {
        let what = match iterator {
            Some(i) => {
                let mut map = HashMap::new();
                for store in i.get_storage() {
                    map.insert(store.index, store);
                }
                WhatStrategy::Task(i, map)
            }
            None => WhatStrategy::Direct,
        };

        Executor { sender, what, stop }
    }

    pub(crate) fn attach(&mut self, num: usize, train_observer: Tx<Train>) {
        self.sender.add(num, train_observer);
    }

    pub(crate) fn detach(&mut self, num: usize) {
        self.sender.remove(num);
    }

    pub fn execute(&mut self, train: Train) {
        if train.values.is_empty() {
            warn!("Train is empty incoming");
            return;
        }

        let train = match &mut self.what {
            WhatStrategy::Task(iter, storages) => {
                let marks = train.marks.clone();
                let event_time = train.event_time;

                // load values
                storages
                    .get_mut(&train.last())
                    .unwrap()
                    .append(train.values);

                let mut train = iter.drain_to_train(self.stop);
                train.event_time = event_time;
                train.marks = marks;
                train
            }
            WhatStrategy::Direct => train,
        };

        if train.values.is_empty() {
            warn!("Train is empty {}", self.stop);
            return;
        }

        let train = train.mark(self.stop); // mark current as last stop

        self.sender.send(train.flag(self.stop));
    }
}

pub struct Direct {
    sender: Sender,
    stop: usize,
}

#[derive(Clone, Default)]
pub struct IdentityIterator {
    values: VecDeque<Value>,
    storage: ValueStore,
}

impl IdentityIterator {
    pub fn new() -> Self {
        IdentityIterator {
            values: Default::default(),
            storage: ValueStore::new(),
        }
    }

    fn load(&mut self) {
        self.drain().into_iter().for_each(|v| {
            self.values.push_back(v);
        });
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
    fn get_storage(&self) -> Vec<ValueStore> {
        vec![self.storage.clone()]
    }

    fn drain_to_train(&mut self, _stop: usize) -> Train {
        self.load();
        Train::new(self.values.drain(..).collect())
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(IdentityIterator {
            values: self.values.clone(),
            storage: self.storage.clone(),
        })
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}
