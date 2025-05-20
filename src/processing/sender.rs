use std::collections::HashMap;
use tracing::warn;
use crate::processing::train::Train;
use crate::util::Tx;

#[derive(Clone, Default)]
pub struct Sender {
    outs: HashMap<usize, Tx<Train>>,
}

impl Sender {
    pub fn new(num: usize,sender: Tx<Train>) -> Self {
        Sender{outs: HashMap::from([(num, sender)])}
    }
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

    pub(crate) fn remove(&mut self, id: usize) {
        self.outs.remove(&id);
    }

    pub(crate) fn send(&self, train: Train) {
        for out in &self.outs {
            if out.1.len() > 1000 {
                warn!("too large")
            }
            out.1.send(train.clone()).expect(&("Error on :".to_owned() + &out.0.to_string()));
        }
    }
}