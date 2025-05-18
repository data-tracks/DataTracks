use crate::processing::block::Block::{All, Non, Specific};
use crate::processing::train::MutWagonsFunc;
use crate::processing::Train;
use crate::value::Value;
use std::collections::hash_map::Drain;
use std::collections::HashMap;
use tracing::log::debug;
use crate::processing::sender::Sender;
use crate::util::Tx;

pub enum Block {
    Non(NonBlock),
    Specific(SpecificBlock),
    All(AllBlock),
}


impl Block {
    pub fn new(inputs: Vec<usize>, blocks: Vec<usize>, next: MutWagonsFunc, sender: Sender) -> Self {
        if blocks.is_empty() {
            return Non(NonBlock { func: next, sender });
        } else if same_vecs(&blocks, &inputs) {
            return All(AllBlock::new(inputs, next, sender));
        }
        Specific(SpecificBlock::new( inputs, blocks, next, sender ))
    }

    pub(crate) fn add(&mut self, num: usize, send: Tx<Train>) {
        match self {
            Non(n) => n.sender.add(num, send),
            Specific(s) => s.sender.add(num, send),
            All(a) => a.sender.add(num, send),
        }
    }

    pub(crate) fn remove(&mut self, num: usize) {
        match self {
            Non(n) => n.sender.remove(num),
            Specific(s) => s.sender.remove(num),
            All(a) => a.sender.remove(num)
        }
    }

    pub fn next(&mut self, train: Train) {
        match self {
            Non(n) => n.next(train),
            Specific(s) => s.next(train),
            All(a) => a.next(train),
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
    func: MutWagonsFunc,
    sender: Sender
}

impl NonBlock {
    fn next(&mut self, train: Train) {
        self.sender.send((self.func)(&mut vec![train]))
    }
}

pub struct SpecificBlock {
    input: Vec<usize>,
    blocks: Vec<usize>,
    func: MutWagonsFunc,
    buffer: HashMap<usize, Vec<Value>>,
    sender: Sender
}

impl SpecificBlock {

    fn new(input: Vec<usize>, blocks: Vec<usize>, func: MutWagonsFunc, sender: Sender) -> Self{
        let mut buffer = HashMap::new();
        blocks.iter().for_each(|b| {
            buffer.insert(*b, vec![]);
        });
        SpecificBlock{input, blocks, func, buffer, sender}
    }
    fn next(&mut self, train: Train) {

        let mark = train.last();
        self.buffer.entry(mark).or_default().append(&mut train.values.unwrap());
        if !self.blocks.contains(&mark) {
            debug!("block{:?}", self.buffer.clone());
            let mut trains = merge_buffer(self.buffer.drain());

            self.sender.send((self.func)(&mut trains))
        }
    }


}

fn merge_buffer(drain: Drain<usize, Vec<Value>>) -> Vec<Train> {
    let mut trains = vec![];
    for (last, values) in drain {
        trains.push(Train::new(values));
    }
    trains
}

pub struct AllBlock {
    input: Vec<usize>,
    func: MutWagonsFunc,
    buffer: HashMap<usize, Vec<Value>>,
    switch: HashMap<usize, bool>,
    sender: Sender
}



impl AllBlock {

    fn new(input: Vec<usize>, func: MutWagonsFunc, sender: Sender) -> Self{
        let mut buffer = HashMap::new();
        let mut switch = HashMap::new();
        input.iter().for_each(|i|{
            buffer.insert(*i, vec![]);
            switch.insert(*i, false);
        });

        AllBlock{input, func, buffer, switch, sender}
    }
    fn next(&mut self, train: Train) {
        let watermark = train.last();
        self.buffer.entry(watermark).or_default().append(&mut train.values.unwrap());
        self.switch.insert(watermark, true);
        if self.switch.iter().all(|(_i,s)| *s) {
            (self.func)(&mut merge_buffer(self.buffer.drain()));

            self.input.iter().for_each(|i|{
                self.buffer.insert(*i, vec![]);
                self.switch.insert(*i, false);
            });
        }
    }
}
