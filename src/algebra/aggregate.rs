use crate::algebra::{Algebra, AlgebraType, BoxedIterator, ValueIterator};
use crate::processing::Train;
use crate::value::Value;

#[derive(Clone)]
pub struct Aggregate {
    input: Box<AlgebraType>,
    aggregates: AggFunction,
}

impl Aggregate {
    pub fn new(input: Box<AlgebraType>, aggregates: AggFunction) -> Aggregate {
        Aggregate { input, aggregates }
    }
}


impl Algebra for Aggregate {
    type Iterator = AggIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        let iter = self.input.derive_iterator();
        AggIterator::new(iter, self.aggregates.clone())
    }
}


pub struct AggIterator {
    input: BoxedIterator,
    values: Vec<Value>,
    aggregates: AggFunction,
    reloaded: bool,
}

impl AggIterator {
    pub fn new(input: BoxedIterator, aggregates: AggFunction) -> AggIterator {
        AggIterator { input, values: vec![], aggregates, reloaded: false }
    }

    pub(crate) fn reload_values(&mut self) {
        while let Some(value) = self.input.next() {
            self.aggregates.load(value);
        }
        self.values.append(&mut self.aggregates.get());


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
        Box::new(AggIterator::new(self.input.clone(), self.aggregates.clone()))
    }
}

trait ValueLoader {
    fn load(&mut self, value: Value);

    fn get(&self) -> Vec<Value>;
}


#[derive(Clone)]
pub enum AggFunction {
    Count(CountOperator)
}

impl ValueLoader for AggFunction {
    fn load(&mut self, value: Value) {
        match self {
            AggFunction::Count(c) => c.load(value),
        }
    }

    fn get(&self) -> Vec<Value> {
        match self {
            AggFunction::Count(c) => c.get(),
        }
    }
}


#[derive(Clone)]
pub struct CountOperator {
    count: usize,
}

impl ValueLoader for CountOperator {
    fn load(&mut self, _value: Value) {
        self.count += 1;
    }

    fn get(&self) -> Vec<Value> {
        vec![Value::int(self.count as i64)]
    }
}
