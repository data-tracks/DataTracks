use std::collections::HashMap;

use crossbeam::channel;

use crate::processing::train::Train;

#[derive(Clone)]
pub(crate) struct Sender {
    outs: HashMap<i64, channel::Sender<Train>>,
}

impl Default for Sender {
    fn default() -> Self {
        Sender { outs: HashMap::new() }
    }
}

impl Sender {
    pub(crate) fn add(&mut self, id: i64, sender: channel::Sender<Train>) {
        self.outs.insert(id, sender.into());
    }

    pub(crate) fn send(&self, train: Train) {
        for out in &self.outs {
            out.1.send(train.clone()).expect(&("Error on :".to_owned() + &out.0.to_string()));
        }
    }
}