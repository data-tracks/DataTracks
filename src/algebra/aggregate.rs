use crate::algebra::algebra::BoxedValueLoader;
use crate::algebra::function::Implementable;
use crate::algebra::operator::AggOp;
use crate::algebra::{Algebra, AlgebraType, BoxedIterator, BoxedValueHandler, Operator, ValueIterator};
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
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
        Aggregate {
            input,
            aggregates: aggregates.into_iter().map(|(op, ops)| {
                let ops = match ops {
                    mut ops if ops.len() == 1 => {
                        ops.pop().unwrap()
                    }
                    ops => {
                        Operator::combine(ops)
                    }
                };
                (op, ops)
            }).collect(),
            group: group.unwrap_or(Operator::literal(Value::bool(true))),
        }
    }
}


impl Algebra for Aggregate {
    type Iterator = AggIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        let iter = self.input.derive_iterator();
        let group = self.group.implement().unwrap();
        let aggregates = self.aggregates.iter().map(|(a, o)| (a.implement().unwrap(), o.implement().unwrap())).collect();
        AggIterator::new(iter, aggregates, group)
    }

    fn derive_input_layout(&self) -> Layout {
        todo!()
    }

    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Layout {
        todo!()
    }
}


pub struct AggIterator {
    input: BoxedIterator,
    groups: HashMap<u64, Vec<Value>>,
    hashes: HashMap<u64, Value>,
    values: Vec<Value>,
    hasher: BoxedValueHandler,
    aggregates: Vec<(BoxedValueLoader, BoxedValueHandler)>,
    reloaded: bool,
}

impl AggIterator {
    pub fn new(input: BoxedIterator, aggregates: Vec<(BoxedValueLoader, BoxedValueHandler)>, group: BoxedValueHandler) -> AggIterator {
        AggIterator { input, groups: Default::default(), hashes: Default::default(), values: vec![], hasher: group, aggregates, reloaded: false }
    }

    pub(crate) fn reload_values(&mut self) {
        self.values.clear();
        self.groups.clear();
        self.hashes.clear();


        for value in self.input.by_ref() {
            let mut hasher = DefaultHasher::new();
            let keys = self.hasher.process(&value);

            keys.hash(&mut hasher);

            let hash = hasher.finish();

            self.hashes.entry(hash).or_insert(keys);
            self.groups.entry(hash).or_default().push(value);
        }

        for (hash, values) in &self.groups {
            for value in values {
                for (ref mut agg, op) in &mut self.aggregates {
                    agg.load(&op.process(value))
                }
            }
            let mut values = vec![];
            for (agg, _) in &self.aggregates {
                values.push(agg.get());
            }
            if values.len() == 1 {
                self.values.push(values.pop().unwrap());
            } else if values.is_empty() {
                self.values.push(self.hashes.get(hash).unwrap().clone());
            } else {
                self.values.push(values.into());
            }
        }

        self.reloaded = true;
    }
}

impl Iterator for AggIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(value) = self.values.pop() {
                return Some(value);
            } else if !self.reloaded {
                self.reload_values();
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
        Box::new(AggIterator::new(self.input.clone(), self.aggregates.iter().map(|(a, o)| ((*a).clone(), (*o).clone())).collect(), self.hasher.clone()))
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        let input = self.input.enrich(transforms);

        if let Some(input) = input {
            self.input = input;
        };
        None
    }
}

pub trait ValueLoader {
    fn clone(&self) -> BoxedValueLoader;

    fn load(&mut self, value: &Value);

    fn get(&self) -> Value;
}


#[derive(Clone, Debug)]
pub struct CountOperator {
    count: usize,
}

impl CountOperator {
    pub fn new() -> Self {
        CountOperator { count: 0 }
    }
}

impl ValueLoader for CountOperator {
    fn clone(&self) -> BoxedValueLoader {
        Box::new(CountOperator::new())
    }

    fn load(&mut self, _value: &Value) {
        self.count += 1;
    }

    fn get(&self) -> Value {
        Value::int(self.count as i64)
    }
}

#[derive(Clone, Debug)]
pub struct SumOperator {
    sum: Value,
}

impl SumOperator {
    pub fn new() -> Self {
        SumOperator { sum: Value::float(0.0) }
    }
}

impl ValueLoader for SumOperator {
    fn clone(&self) -> BoxedValueLoader {
        Box::new(SumOperator::new())
    }

    fn load(&mut self, value: &Value) {
        self.sum += value.clone();
    }

    fn get(&self) -> Value {
        self.sum.clone()
    }
}

#[derive(Clone, Debug)]
pub struct AvgOperator {
    sum: Value,
    count: usize,
}

impl AvgOperator {
    pub fn new() -> Self {
        AvgOperator { sum: Value::float(0.0), count: 0 }
    }
}

impl ValueLoader for AvgOperator {
    fn clone(&self) -> BoxedValueLoader {
        Box::new(SumOperator::new())
    }

    fn load(&mut self, value: &Value) {
        self.sum += value.clone();
        self.count += 1;
    }

    fn get(&self) -> Value {
        &self.sum.clone() / &Value::float(self.count as f64)
    }
}
