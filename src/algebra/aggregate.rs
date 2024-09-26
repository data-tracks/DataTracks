use crate::algebra::{Algebra, AlgebraType, BoxedIterator, Operator, ValueIterator};
use crate::processing::Train;
use crate::value::Value;

#[derive(Clone)]
pub struct Aggregate {
    input: Box<AlgebraType>,
    aggregates: Vec<Aggregation>,
}

impl Aggregate {
    pub fn new(input: Box<AlgebraType>, aggregates: Vec<Aggregation>) -> Aggregate {
        Aggregate { input, aggregates }
    }
}


impl Algebra for Aggregate {
    type Iterator = AggIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        todo!()
    }
}


#[derive(Clone)]
pub struct Aggregation {
    key: fn(Value) -> Value,
    operation: Operator,
}

pub struct AggIterator {}

impl Iterator<Item=Value> for AggIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl ValueIterator for AggIterator {
    fn load(&mut self, trains: Vec<Train>) {
        todo!()
    }

    fn clone(&self) -> BoxedIterator {
        todo!()
    }
}


pub enum AggFunction {
    Count(CountOperator)
}


pub struct CountOperator {
    count: usize,
}

