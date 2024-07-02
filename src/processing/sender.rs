use std::collections::HashMap;

use crate::processing::train::Train;
use crate::util::Tx;

#[derive(Clone)]
pub(crate) struct Sender {
    outs: HashMap<i64, Tx<Train>>,
}

impl Default for Sender {
    fn default() -> Self {
        Sender { outs: HashMap::new() }
    }
}

impl Sender {
    pub(crate) fn add(&mut self, id: i64, sender: Tx<Train>) {
        self.outs.insert(id, sender.into());
    }

    pub(crate) fn send(&self, train: Train) {
        for out in &self.outs {
            out.1.send(train.clone()).expect(&("Error on :".to_owned() + &out.0.to_string()));
        }
    }
}