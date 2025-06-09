use crate::util::Tx;
use std::collections::HashMap;
use tracing::warn;
use value::train::Train;

#[derive(Clone, Default)]
pub struct Sender {
    outs: HashMap<usize, Tx<Train>>,
}

impl Sender {
    pub fn new(num: usize, sender: Tx<Train>) -> Self {
        Sender {
            outs: HashMap::from([(num, sender)]),
        }
    }
}

impl Sender {
    pub(crate) fn add(&mut self, id: usize, sender: Tx<Train>) {
        self.outs.insert(id, sender);
    }

    pub(crate) fn remove(&mut self, id: usize) {
        self.outs.remove(&id);
    }

    pub fn send(&self, train: Train) {
        for out in &self.outs {
            if out.1.len() > 100 {
                warn!("too large {}, size {}", out.1.name(), out.1.len());
            }
            out.1.send(train.clone());
        }
    }
}
