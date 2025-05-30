use crate::processing::block::Block::{All, Non, Specific};
use crate::processing::Train;
use value::Value;
use std::collections::hash_map::Drain;
use std::collections::HashMap;
use tracing::log::debug;
use crate::processing::platform::Step;
use crate::util::Tx;

pub enum Block {
    Non(NonBlock),
    Specific(SpecificBlock),
    All(AllBlock),
}


impl Block {
    pub fn new(inputs: Vec<usize>, blocks: Vec<usize>, next: Box<dyn Step>) -> Self {
        if blocks.is_empty() {
            return Non(NonBlock { next_step: next });
        } else if same_vecs(&blocks, &inputs) {
            return All(AllBlock::new(inputs, next));
        }
        Specific(SpecificBlock::new( inputs, blocks, next ))
    }

    pub(crate) fn attach(&mut self, num: usize, send: Tx<Train>) {
        match self {
            Non(n) => n.next_step.attach(num, send),
            Specific(s) => s.next.attach(num, send),
            All(a) => a.next_step.attach(num, send),
        }
    }

    pub(crate) fn detach(&mut self, num: usize) {
        match self {
            Non(n) => n.next_step.detach(num),
            Specific(s) => s.next.detach(num),
            All(a) => a.next_step.detach(num)
        }
    }

    pub fn next(&mut self, train: Train) {
        match self {
            Non(n) => n.apply(train),
            Specific(s) => s.apply(train),
            All(a) => a.apply(train),
        }
    }
}

impl Step for Block {
    fn apply(&mut self, train: Train) {
        match self {
            Non(n) => n.apply(train),
            Specific(s) => s.apply(train),
            All(a) => a.apply(train)
        }
    }

    fn detach(&mut self, num: usize) {
        match self {
            Non(n) => n.next_step.detach(num),
            Specific(s) => s.next.detach(num),
            All(a) => a.next_step.detach(num)
        }
    }

    fn attach(&mut self, num: usize, tx: Tx<Train>) {
        match self {
            Non(n) => n.next_step.attach(num, tx),
            Specific(s) => s.next.attach(num, tx),
            All(a) => a.next_step.attach(num, tx)
        }
    }
}

fn same_vecs(a: &Vec<usize>, b: &Vec<usize>) -> bool {
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

    true
}



pub struct NonBlock {
    next_step: Box<dyn Step>,
}


impl Step for NonBlock {
    fn apply(&mut self, trains: Train) {
        self.next_step.apply(trains);
    }

    fn detach(&mut self, num: usize) {
        self.next_step.detach(num)
    }

    fn attach(&mut self, num: usize, tx: Tx<Train>) {
        self.next_step.attach(num, tx)
    }
}

pub struct SpecificBlock {
    input: Vec<usize>,
    blocks: Vec<usize>,
    buffer: HashMap<usize, Vec<Value>>,
    next: Box<dyn Step>,
}

impl SpecificBlock {

    fn new(input: Vec<usize>, blocks: Vec<usize>, next: Box<dyn Step>) -> Self{
        let mut buffer = HashMap::new();
        blocks.iter().for_each(|b| {
            buffer.insert(*b, vec![]);
        });
        SpecificBlock{input, blocks, buffer, next}
    }
    
}

impl Step for SpecificBlock {
    fn apply(&mut self, train: Train) {
        let mark = train.last();
        self.buffer.entry(mark).or_default().append(&mut train.values.unwrap());
        if !self.blocks.contains(&mark) {
            debug!("block{:?}", self.buffer.clone());
            let mut trains = merge_buffer(self.buffer.drain());

            self.next.apply(trains.into())
        }
    }

    fn detach(&mut self, num: usize) {
        self.next.detach(num);
    }

    fn attach(&mut self, num: usize, tx: Tx<Train>) {
        self.next.attach(num, tx);
    }
}

fn merge_buffer(drain: Drain<usize, Vec<Value>>) -> Vec<Train> {
    let mut trains = vec![];
    for (last, values) in drain {
        trains.push(Train::new(values).mark(last));
    }
    trains
}

pub struct AllBlock {
    input: Vec<usize>,
    buffer: HashMap<usize, Vec<Value>>,
    switch: HashMap<usize, bool>,
    next_step: Box<dyn Step>,
}



impl AllBlock {

    fn new(input: Vec<usize>, next_step: Box<dyn Step>) -> Self{
        let mut buffer = HashMap::new();
        let mut switch = HashMap::new();
        input.iter().for_each(|i|{
            buffer.insert(*i, vec![]);
            switch.insert(*i, false);
        });

        AllBlock{input, buffer, switch, next_step}
    }
    fn apply(&mut self, train: Train) {
        let watermark = train.last();
        self.buffer.entry(watermark).or_default().append(&mut train.values.unwrap());
        self.switch.insert(watermark, true);
        if self.switch.iter().all(|(_i,s)| *s) {
            self.next_step.apply(merge_buffer(self.buffer.drain()).into());

            self.input.iter().for_each(|i|{
                self.buffer.insert(*i, vec![]);
                self.switch.insert(*i, false);
            });
        }
    }
}
