use std::collections::HashMap;
use std::sync::mpsc;
use crate::processing::train::Train;

pub(crate) struct Sender {
    outs: HashMap<i64, mpsc::Sender<Train>>,
}

impl Sender {
    pub(crate) fn new() -> Self {
        Sender { outs: HashMap::new() }
    }

    pub(crate) fn add(&mut self, id: i64, sender: mpsc::Sender<Train>){
        self.outs.insert(id, sender);
    }

    pub(crate) fn send(&self, train: Train) {
        for out in &self.outs {
            out.1.send(train.clone()).unwrap();
        }
    }
}