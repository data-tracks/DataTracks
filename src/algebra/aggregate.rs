use crate::algebra::algebra::BoxedValueLoader;
use crate::algebra::function::Implementable;
use crate::algebra::operator::AggOp;
use crate::algebra::{Algebra, AlgebraType, BoxedIterator, BoxedValueHandler, InputFunction, Op, OperationFunction, Operator, ValueIterator};
use crate::processing::Train;
use crate::value::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct Aggregate {
    input: Box<AlgebraType>,
    aggregates: Vec<(AggOp, Operator)>,
    group: Operator,
}

impl Aggregate {
    pub fn new(input: Box<AlgebraType>, aggregates: Vec<(AggOp, Vec<Operator>)>, group: Option<Operator>) -> Self {
        let aggregates = aggregates.into_iter().map(|(op, ops)| {
            if ops.len() == 1 {
                (op, ops.get(0).unwrap())
            } else {
                (op, &Operator::Operation(OperationFunction::new(Op::combine(), ops)))
            }
        }).collect();
        Aggregate { input, aggregates, group: group.unwrap_or(Operator::Input(InputFunction::all())) }
    }
}


impl Algebra for Aggregate {
    type Iterator = AggIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        let iter = self.input.derive_iterator();
        let hash = self.group.implement().unwrap();
        let aggregates = self.aggregates.iter().map(|a| a.implement()).collect();
        AggIterator::new(iter, aggregates, hash)
    }
}


pub struct AggIterator {
    input: BoxedIterator,
    groups: HashMap<u64, Vec<Value>>,
    hashes: HashMap<u64, Value>,
    values: Vec<Value>,
    hasher: BoxedValueHandler,
    aggregates: Vec<BoxedValueLoader>,
    reloaded: bool,
}

impl AggIterator {
    pub fn new(input: BoxedIterator, aggregates: Vec<BoxedValueLoader>, hasher: BoxedValueHandler) -> AggIterator {
        AggIterator { input, groups: Default::default(), hashes: Default::default(), values: vec![], hasher, aggregates, reloaded: false }
    }

    pub(crate) fn reload_values(&mut self) {
        self.values.clear();
        self.groups.clear();
        self.hashes.clear();

        let mut hasher = DefaultHasher::new();

        while let Some(value) = self.input.next() {
            let keys = self.hasher.process(&value);
            keys.hash(&mut hasher);
            let hash = hasher.finish();

            self.hashes.entry(hash).or_insert(keys);
            self.groups.entry(hash).or_insert(vec![]).push(value);
        }

        for (_hash, values) in &self.groups {
            for value in values {
                for mut agg in &self.aggregates {
                    agg.load(&value)
                }
            }
            let mut values = vec![];
            for agg in &self.aggregates {
                values.push(agg.get());
            }
            self.values.push(values.into());
        }

        self.reloaded = true;
    }
}

impl Iterator for AggIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.reloaded {
                self.reload_values();
            } else if let Some(value) = self.values.pop() {
                return Some(value);
            } else {
                return None;
            }
        }
    }
}

impl ValueIterator for AggIterator {
    fn load(&mut self, trains: Vec<Train>) {
        self.input.load(trains);
        self.reloaded = false;
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(AggIterator::new(self.input.clone(), self.aggregates.clone(), self.hasher.clone()))
    }
}

pub trait ValueLoader {
    fn load(&mut self, value: &Value);

    fn get(&self) -> Value;
}


#[derive(Clone, Debug)]
pub struct CountOperator {
    count: usize,
}

impl CountOperator {
    pub fn new() -> CountOperator {
        CountOperator { count: 0 }
    }
}

impl ValueLoader for CountOperator {
    fn load(&mut self, _value: &Value) {
        self.count += 1;
    }

    fn get(&self) -> Value {
        Value::int(self.count as i64)
    }
}
