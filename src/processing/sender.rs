use std::collections::HashMap;

use crate::processing::train::Train;
use crate::util::Tx;

#[derive(Clone, Default)]
pub struct Sender {
    outs: HashMap<usize, Tx<Train>>,
}

impl Sender {
    pub(crate) fn send_to(&self, num: usize, train: Train) {
        self.outs.get(&num).unwrap().send(train).unwrap();
    }
}

impl Sender {
    pub(crate) fn add(&mut self, id: usize, sender: Tx<Train>) {
        self.outs.insert(id, sender);
    }

    pub(crate) fn send(&self, train: Train) {
        for out in &self.outs {
            out.1.send(train.clone()).expect(&("Error on :".to_owned() + &out.0.to_string()));
        }
    }
}