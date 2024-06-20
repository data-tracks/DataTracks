use std::collections::HashMap;

use crate::processing::block::Block::{All, Non, Specific};
use crate::processing::Train;
use crate::value::Value;

pub(crate) enum Block {
    Non(NonBlock),
    Specific(SpecificBlock),
    All(AllBlock),
}


impl Block {
    pub(crate) fn new(inputs: Vec<i64>, blocks: Vec<i64>, next: fn(&mut Vec<Train>)) -> Self {
        if blocks.is_empty() {
            return Non(NonBlock { func: next });
        } else if same_vecs(&blocks, &inputs) {
            return All(AllBlock { input, func: next, buffer: HashMap::new() });
        }
        Specific(SpecificBlock { blocks, func: next, buffer: HashMap::new() })
    }

    pub(crate) fn next(&self, train: Train) {
        match self {
            Non(n) => n.next(train),
            Specific(s) => s.next(train),
            All(a) => a.next(train)
        }
    }
}

fn same_vecs(a: &Vec<i64>, b: &Vec<i64>) -> bool {
    for entry in a {
        if !b.contains(entry) {
            return false;
        }
    }
    for entry in b {
        if !a.contains(entry) {
            return false;
        }
    }

    return true;
}

pub(crate) struct NonBlock {
    func: fn(&mut Vec<Train>),
}

impl NonBlock {
    fn next(&self, train: Train) {
        (self.func)(&mut vec![train])
    }
}

pub(crate) struct SpecificBlock {
    pub(crate) blocks: Vec<i64>,
    func: fn(&mut Vec<Train>),
    buffer: HashMap<i64, Vec<Value>>,
}

impl SpecificBlock {
    fn next(&mut self, train: Train) {
        if !self.blocks.contains(&train.last) {
            let mut trains = vec![];
            for (last, values) in self.buffer.iter_mut() {
                trains.push(Train::new(last.clone(), values.clone()));
                values.clear();
            }

            (self.func)(&mut trains)
        } else {
            self.buffer.insert(train.last.clone(), train.values.unwrap());
        }
    }
}

pub(crate) struct AllBlock {
    input: Vec<i64>,
    func: fn(&mut Vec<Train>),
    buffer: HashMap<i64, Vec<Value>>,
}

impl AllBlock {
    fn next(&self, train: Train) {
        (self.func)(train)
    }
}