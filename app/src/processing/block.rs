use crate::processing::block::Block::{All, Non, Specific};
use crate::processing::Train;
use std::collections::hash_map::Drain;
use std::collections::HashMap;
use value::Value;

pub enum Block {
    Non(NonBlock),
    Specific(SpecificBlock),
    All(AllBlock),
}


impl Block {
    pub fn new(inputs: Vec<usize>, blocks: Vec<usize>) -> Self {
        if blocks.is_empty() {
            return Non(NonBlock{});
        } else if same_vecs(&blocks, &inputs) {
            return All(AllBlock::new(inputs));
        }
        Specific(SpecificBlock::new( inputs, blocks ))
    }

    pub fn next(&mut self, train: Train) {
        match self {
            Non(n) => n.apply(train),
            Specific(s) => s.apply(train),
            All(a) => a.apply(train),
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
}


pub struct SpecificBlock {
    input: Vec<usize>,
    blocks: Vec<usize>,
    buffer: HashMap<usize, Vec<Value>>,
}

impl SpecificBlock {

    fn new(input: Vec<usize>, blocks: Vec<usize>) -> Self{
        let mut buffer = HashMap::new();
        blocks.iter().for_each(|b| {
            buffer.insert(*b, vec![]);
        });
        SpecificBlock{input, blocks, buffer}
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
}



impl AllBlock {

    fn new(input: Vec<usize>) -> Self{
        let mut buffer = HashMap::new();
        let mut switch = HashMap::new();
        input.iter().for_each(|i|{
            buffer.insert(*i, vec![]);
            switch.insert(*i, false);
        });

        AllBlock{input, buffer, switch}
    }
}
