use std::collections::hash_map::Drain;
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
    pub(crate) fn new(inputs: Vec<i64>, blocks: Vec<i64>, next: Box<dyn Fn(&mut Vec<Train>)>) -> Self {
        if blocks.is_empty() {
            return Non(NonBlock { func: next });
        } else if same_vecs(&blocks, &inputs) {
            return All(AllBlock::new(inputs, next));
        }
        Specific(SpecificBlock::new( inputs, blocks, next ))
    }

    pub(crate) fn next(&mut self, train: Train) {
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
    func: Box<dyn Fn(&mut Vec<Train>)>,
}

impl NonBlock {
    fn next(&self, train: Train) {
        (self.func)(&mut vec![train])
    }
}

pub(crate) struct SpecificBlock {
    input: Vec<i64>,
    blocks: Vec<i64>,
    func: Box< dyn Fn(&mut Vec<Train>)>,
    buffer: HashMap<i64, Vec<Value>>,
}

impl SpecificBlock {

    fn new(input: Vec<i64>, blocks: Vec<i64>, func: Box<dyn Fn(&mut Vec<Train>)>) -> Self{
        let mut buffer = HashMap::new();
        blocks.iter().for_each(|b| {
            buffer.insert(b.clone(), vec![]);
        });
        SpecificBlock{input, blocks, func, buffer}
    }
    fn next(&mut self, train: Train) {
        self.buffer.entry(train.last).or_insert_with(Vec::new).append(&mut train.values.unwrap());
        if !self.blocks.contains(&train.last) {
            let mut trains = merge_buffer(self.buffer.drain());

            (self.func)(&mut trains)
        }
    }


}

fn merge_buffer(drain: Drain<i64, Vec<Value>>) -> Vec<Train> {
    let mut trains = vec![];
    for (last, values) in drain {
        trains.push(Train::new(last.clone(), values));
    }
    trains
}

pub(crate) struct AllBlock {
    input: Vec<i64>,
    func: Box<dyn Fn(&mut Vec<Train>)>,
    buffer: HashMap<i64, Vec<Value>>,
    switch: HashMap<i64, bool>
}



impl AllBlock {

    fn new(input: Vec<i64>, func: Box<dyn Fn(&mut Vec<Train>)>) -> Self{
        let mut buffer = HashMap::new();
        let mut switch = HashMap::new();
        input.iter().for_each(|i|{
            buffer.insert(i.clone(), vec![]);
            switch.insert(i.clone(), false);
        });

        AllBlock{input, func, buffer, switch}
    }
    fn next(&mut self, train: Train) {
        self.buffer.entry(train.last).or_insert_with(Vec::new).append(&mut train.values.unwrap());
        self.switch.insert(train.last, true);
        if self.switch.iter().all(|(i,s)| s.clone()) {
            (self.func)(&mut merge_buffer(self.buffer.drain()));

            self.input.iter().for_each(|i|{
                self.buffer.insert(i.clone(), vec![]);
                self.switch.insert(i.clone(), false);
            });
        }
    }
}