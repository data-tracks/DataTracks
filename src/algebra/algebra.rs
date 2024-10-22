use crate::algebra::aggregate::{Aggregate, ValueLoader};
use crate::algebra::filter::Filter;
use crate::algebra::join::Join;
use crate::algebra::project::Project;
use crate::algebra::scan::Scan;
use crate::algebra::union::Union;
use crate::algebra::variable::VariableScan;
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
use crate::value::Value;
use std::collections::HashMap;

pub type BoxedIterator = Box<dyn ValueIterator<Item=Value> + Send + 'static>;

pub type BoxedValueHandler = Box<dyn ValueHandler + Send + 'static>;

pub type BoxedValueLoader = Box<dyn ValueLoader + Send + 'static>;

#[derive(Clone)]
pub enum AlgebraType {
    Scan(Scan),
    Project(Project),
    Filter(Filter),
    Join(Join),
    Union(Union),
    Aggregate(Aggregate),
    Variable(VariableScan),
}


impl Algebra for AlgebraType {
    type Iterator = BoxedIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        match self {
            AlgebraType::Scan(s) => Box::new(s.derive_iterator()),
            AlgebraType::Project(p) => Box::new(p.derive_iterator()),
            AlgebraType::Filter(f) => Box::new(f.derive_iterator()),
            AlgebraType::Join(j) => Box::new(j.derive_iterator()),
            AlgebraType::Union(u) => Box::new(u.derive_iterator()),
            AlgebraType::Aggregate(a) => Box::new(a.derive_iterator()),
            AlgebraType::Variable(s) => Box::new(s.derive_iterator())
        }
    }

    fn derive_input_layout(&self) -> Layout {
        match self {
            AlgebraType::Scan(s) => s.derive_input_layout(),
            AlgebraType::Project(p) => p.derive_input_layout(),
            AlgebraType::Filter(f) => f.derive_input_layout(),
            AlgebraType::Join(j) => j.derive_input_layout(),
            AlgebraType::Union(u) => u.derive_input_layout(),
            AlgebraType::Aggregate(a) => a.derive_input_layout(),
            AlgebraType::Variable(v) => v.derive_input_layout()
        }
    }

    fn derive_output_layout(&self) -> Layout {
        match self {
            AlgebraType::Scan(s) => s.derive_output_layout(),
            AlgebraType::Project(p) => p.derive_output_layout(),
            AlgebraType::Filter(f) => f.derive_output_layout(),
            AlgebraType::Join(j) => j.derive_output_layout(),
            AlgebraType::Union(u) => u.derive_output_layout(),
            AlgebraType::Aggregate(a) => a.derive_output_layout(),
            AlgebraType::Variable(v) => v.derive_output_layout()
        }
    }
}

pub trait Algebra: Clone {
    type Iterator: Iterator<Item=Value> + Send + 'static;
    fn derive_iterator(&mut self) -> Self::Iterator;

    fn derive_input_layout(&self) -> Layout;

    fn derive_output_layout(&self) -> Layout;

}

pub fn build_iterator(mut algebra: AlgebraType) -> Result<BoxedIterator, String> {
    Ok(algebra.derive_iterator())
}

pub trait RefHandler: Send {
    fn process(&self, stop: i64, wagons: Vec<Train>) -> Vec<Train>;

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static>;
}

pub trait ValueHandler: Send {
    fn process(&self, value: &Value) -> Value;

    fn clone(&self) -> BoxedValueHandler;
}


pub trait ValueIterator: Iterator<Item=Value> + Send + 'static {
    fn load(&mut self, trains: Vec<Train>);

    fn drain(&mut self) -> Vec<Value> {
        self.into_iter().collect()
    }

    fn drain_to_train(&mut self, stop: i64) -> Train {
        Train::new(stop, self.drain())
    }

    fn clone(&self) -> BoxedIterator;

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator>;

}

