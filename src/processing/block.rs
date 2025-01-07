use crate::processing::block::Block::{All, Non, Specific};
use crate::processing::train::MutWagonsFunc;
use crate::processing::Train;
use crate::value::Value;
use std::collections::hash_map::Drain;
use std::collections::HashMap;
use tracing::log::debug;

pub enum Block {
    Non(NonBlock),
    Specific(SpecificBlock),
    All(AllBlock),
}




impl Block {
    pub fn new(inputs: Vec<i64>, blocks: Vec<i64>, next: MutWagonsFunc) -> Self {
        if blocks.is_empty() {
            return Non(NonBlock { func: next });
        } else if same_vecs(&blocks, &inputs) {
            return All(AllBlock::new(inputs, next));
        }
        Specific(SpecificBlock::new( inputs, blocks, next ))
    }

    pub fn next(&mut self, train: Train) {
        match self {
            Non(n) => n.next(train),
            Specific(s) => s.next(train),
            All(a) => a.next(train),
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

    true
}



pub(crate) struct NonBlock {
    func: MutWagonsFunc,
}

impl NonBlock {
    fn next(&mut self, train: Train) {
        (self.func)(&mut vec![train])
    }
}

pub(crate) struct SpecificBlock {
    input: Vec<i64>,
    blocks: Vec<i64>,
    func: MutWagonsFunc,
    buffer: HashMap<i64, Vec<Value>>,
}

impl SpecificBlock {

    fn new(input: Vec<i64>, blocks: Vec<i64>, func: MutWagonsFunc) -> Self{
        let mut buffer = HashMap::new();
        blocks.iter().for_each(|b| {
            buffer.insert(*b, vec![]);
        });
        SpecificBlock{input, blocks, func, buffer}
    }
    fn next(&mut self, train: Train) {

        self.buffer.entry(train.last).or_default().append(&mut train.values.unwrap());
        if !self.blocks.contains(&train.last) {
            debug!("block{:?}", self.buffer.clone());
            let mut trains = merge_buffer(self.buffer.drain());

            (self.func)(&mut trains)
        }
    }


}

fn merge_buffer(drain: Drain<i64, Vec<Value>>) -> Vec<Train> {
    let mut trains = vec![];
    for (last, values) in drain {
        trains.push(Train::new(last, values));
    }
    trains
}

pub(crate) struct AllBlock {
    input: Vec<i64>,
    func: MutWagonsFunc,
    buffer: HashMap<i64, Vec<Value>>,
    switch: HashMap<i64, bool>
}



impl AllBlock {

    fn new(input: Vec<i64>, func: MutWagonsFunc) -> Self{
        let mut buffer = HashMap::new();
        let mut switch = HashMap::new();
        input.iter().for_each(|i|{
            buffer.insert(*i, vec![]);
            switch.insert(*i, false);
        });

        AllBlock{input, func, buffer, switch}
    }
    fn next(&mut self, train: Train) {
        self.buffer.entry(train.last).or_default().append(&mut train.values.unwrap());
        self.switch.insert(train.last, true);
        if self.switch.iter().all(|(_i,s)| *s) {
            (self.func)(&mut merge_buffer(self.buffer.drain()));

            self.input.iter().for_each(|i|{
                self.buffer.insert(*i, vec![]);
                self.switch.insert(*i, false);
            });
        }
    }
}

#[cfg(test)]
mod test {
    use crate::processing::block::Block;
    use crate::processing::Train;
    use crate::value::{Dict, Value};
    use std::sync::mpsc::channel;
    use std::time::Instant;

    #[test]
    fn overhead() {
        let (tx, rx) = channel();

        let process = Box::new(move |trains: &mut Vec<Train>| {
            tx.send(trains.clone()).unwrap();
        });
        let mut block = Block::new(vec![], vec![], process);

        let mut trains = vec![];
        let amount = 1000;
        for _ in 0..amount {
            trains.push(Train::new(0, vec![Value::Dict(Dict::from(Value::int(3)))]))
        }

        let instant = Instant::now();
        for train in trains {
            block.next(train)
        }

        for _ in 0..amount {
            rx.recv().unwrap();
        }
        let elapsed = instant.elapsed();

        println!("time total for {} data points, all {:?}, single {:?}", amount, elapsed, elapsed/amount);
    }
}