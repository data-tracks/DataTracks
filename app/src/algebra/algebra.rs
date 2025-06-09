use crate::algebra::aggregate::{Aggregate, ValueLoader};
use crate::algebra::dual::Dual;
use crate::algebra::filter::Filter;
use crate::algebra::join::Join;
use crate::algebra::project::{Project, ProjectIter};
use crate::algebra::scan::IndexScan;
use crate::algebra::set::AlgSet;
use crate::algebra::union::Union;
use crate::algebra::variable::VariableScan;
use crate::algebra::{Operator, Scan};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::optimize::Cost;
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
use crate::util::storage::ValueStore;
use std::collections::HashMap;
use value::Value;

pub type BoxedIterator = Box<dyn ValueIterator<Item = Value> + Send + 'static>;

pub type BoxedValueHandler = Box<dyn ValueHandler + Send + 'static>;

pub type BoxedValueLoader = Box<dyn ValueLoader + Send + 'static>;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AlgebraType {
    Dual(Dual),
    IndexScan(IndexScan),
    TableScan(Scan),
    Project(Project),
    Filter(Filter),
    Join(Join),
    Union(Union),
    Aggregate(Aggregate),
    Variable(VariableScan),
    Set(AlgSet),
}

impl AlgebraType {
    pub(crate) fn calc_cost(&self) -> Cost {
        match self {
            AlgebraType::Dual(_) => Cost::new(1),
            AlgebraType::IndexScan(_) => Cost::new(1),
            AlgebraType::TableScan(_) => Cost::new(1),
            AlgebraType::Project(p) => Cost::new(1) + p.project.calc_cost() + p.input.calc_cost(),
            AlgebraType::Filter(f) => Cost::new(1) + f.condition.calc_cost() + f.input.calc_cost(),
            AlgebraType::Join(j) => Cost::new(2) + j.left.calc_cost() + j.right.calc_cost(),
            AlgebraType::Union(u) => {
                Cost::new(u.inputs.len())
                    + u.inputs
                        .iter()
                        .fold(Cost::default(), |a, b| a * b.calc_cost())
            }
            AlgebraType::Aggregate(a) => {
                Cost::new(1) + Cost::new(a.aggregates.len()) * a.input.calc_cost()
            }
            AlgebraType::Variable(_) => Cost::new(1) + Cost::default(),
            AlgebraType::Set(s) => s.set.iter().fold(Cost::default(), |a, b| {
                let b = b.calc_cost();
                if a < b {
                    a
                } else {
                    b
                }
            }),
        }
    }

    pub fn table(name: String) -> AlgebraType {
        AlgebraType::TableScan(Scan::new(name))
    }

    pub fn project(project: Operator, input: AlgebraType) -> AlgebraType {
        AlgebraType::Project(Project::new(project, input))
    }

    pub fn filter(condition: Operator, input: AlgebraType) -> AlgebraType {
        AlgebraType::Filter(Filter::new(input, condition))
    }
}

impl InputDerivable for AlgebraType {
    fn derive_input_layout(&self) -> Option<Layout> {
        match self {
            AlgebraType::IndexScan(s) => s.derive_input_layout(),
            AlgebraType::Project(p) => p.derive_input_layout(),
            AlgebraType::Filter(f) => f.derive_input_layout(),
            AlgebraType::Join(j) => j.derive_input_layout(),
            AlgebraType::Union(u) => u.derive_input_layout(),
            AlgebraType::Aggregate(a) => a.derive_input_layout(),
            AlgebraType::Variable(v) => v.derive_input_layout(),
            AlgebraType::Dual(d) => d.derive_input_layout(),
            AlgebraType::TableScan(t) => t.derive_input_layout(),
            AlgebraType::Set(s) => s.derive_input_layout(),
        }
    }
}

impl OutputDerivable for AlgebraType {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        match self {
            AlgebraType::IndexScan(s) => s.derive_output_layout(inputs),
            AlgebraType::Project(p) => p.derive_output_layout(inputs),
            AlgebraType::Filter(f) => f.derive_output_layout(inputs),
            AlgebraType::Join(j) => j.derive_output_layout(inputs),
            AlgebraType::Union(u) => u.derive_output_layout(inputs),
            AlgebraType::Aggregate(a) => a.derive_output_layout(inputs),
            AlgebraType::Variable(v) => v.derive_output_layout(inputs),
            AlgebraType::Dual(d) => d.derive_output_layout(inputs),
            AlgebraType::TableScan(t) => t.derive_output_layout(inputs),
            AlgebraType::Set(s) => s.initial.derive_output_layout(inputs),
        }
    }
}

impl Algebra for AlgebraType {
    type Iterator = BoxedIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        match self {
            AlgebraType::IndexScan(s) => Box::new(s.derive_iterator()),
            AlgebraType::Project(p) => {
                let iter = Box::new(p.derive_iterator());
                match *iter {
                    ProjectIter::ValueProjectIterator(p) => Box::new(p),
                    ProjectIter::ValueSetProjectIterator(p) => Box::new(p),
                }
            }
            AlgebraType::Filter(f) => Box::new(f.derive_iterator()),
            AlgebraType::Join(j) => Box::new(j.derive_iterator()),
            AlgebraType::Union(u) => Box::new(u.derive_iterator()),
            AlgebraType::Aggregate(a) => Box::new(a.derive_iterator()),
            AlgebraType::Variable(s) => Box::new(s.derive_iterator()),
            AlgebraType::Dual(d) => Box::new(d.derive_iterator()),
            AlgebraType::TableScan(t) => Box::new(t.derive_iterator()),
            AlgebraType::Set(s) => s.initial.derive_iterator(),
        }
    }
}

pub trait Algebra: Clone + InputDerivable + OutputDerivable {
    type Iterator: Iterator<Item = Value> + Send + 'static;
    fn derive_iterator(&mut self) -> Self::Iterator;
}

pub fn build_iterator(mut algebra: AlgebraType) -> Result<BoxedIterator, String> {
    Ok(algebra.derive_iterator())
}

pub trait RefHandler: Send {
    fn process(&self, stop: usize, wagons: Vec<Train>) -> Vec<Train>;

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static>;
}

pub trait ValueHandler: Send {
    fn process(&self, value: &Value) -> Value;

    fn clone(&self) -> BoxedValueHandler;
}

pub struct IdentityHandler;

impl IdentityHandler {
    pub fn new() -> BoxedValueHandler {
        Box::new(IdentityHandler {})
    }
}
impl ValueHandler for IdentityHandler {
    fn process(&self, value: &Value) -> Value {
        value.clone()
    }
    fn clone(&self) -> Box<dyn ValueHandler + Send + 'static> {
        Box::new(IdentityHandler)
    }
}

pub trait ValueIterator: Iterator<Item = Value> + Send + 'static {
    fn set_storage(&mut self, storage: ValueStore);

    fn drain(&mut self) -> Vec<Value> {
        self.into_iter().collect()
    }

    fn drain_to_train(&mut self, stop: usize) -> Train {
        Train::new(self.drain()).mark(stop)
    }

    fn clone(&self) -> BoxedIterator;

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator>;
}
