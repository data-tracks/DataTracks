use crate::algebra::BoxedIterator;
use crate::processing::Sender;
use crate::util::Tx;
use crate::util::storage::ValueStore;
use std::collections::HashMap;
use tracing::warn;
use value::train::Train;

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
    pub fn new(stop: usize, iterator: Option<BoxedIterator>, sender: Sender) -> Self {
        let what = match iterator {
            Some(i) => {
                let mut map = HashMap::new();
                for store in i.get_storages() {
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
