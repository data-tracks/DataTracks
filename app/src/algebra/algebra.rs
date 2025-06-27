use crate::algebra::aggregate::{Aggregate, ValueLoader};
use crate::algebra::dual::Dual;
use crate::algebra::filter::Filter;
use crate::algebra::join::Join;
use crate::algebra::project::{Project, ProjectIter};
use crate::algebra::scan::IndexScan;
use crate::algebra::set::AlgSet;
use crate::algebra::sort::Sort;
use crate::algebra::union::Union;
use crate::algebra::variable::VariableScan;
use crate::algebra::{Operator, Scan};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::optimize::Cost;
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
use crate::util::storage::ValueStore;
use std::collections::HashMap;
use std::ops::Mul;
use value::Value;

pub type BoxedIterator = Box<dyn ValueIterator<Item = Value> + Send + 'static>;

pub type BoxedValueHandler = Box<dyn ValueHandler + Send + 'static>;

pub type BoxedValueLoader = Box<dyn ValueLoader + Send + 'static>;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Algebraic {
    Dual(Dual),
    IndexScan(IndexScan),
    Scan(Scan),
    Project(Project),
    Filter(Filter),
    Join(Join),
    Union(Union),
    Aggregate(Aggregate),
    Variable(VariableScan),
    Set(AlgSet),
    Sort(Sort),
}

impl Mul for &Cost {
    type Output = Cost;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Cost::Numeric(a), Cost::Numeric(b)) => Cost::Numeric(a.mul(b)),
            (_, _) => Cost::Infinite,
        }
    }
}

impl Algebraic {
    pub(crate) fn calc_cost(&self) -> Cost {
        match self {
            Algebraic::Dual(_) => Cost::new(1),
            Algebraic::IndexScan(_) => Cost::new(1),
            Algebraic::Scan(_) => Cost::new(1),
            Algebraic::Project(p) => Cost::new(1) + p.project.calc_cost() + p.input.calc_cost(),
            Algebraic::Filter(f) => Cost::new(1) + f.condition.calc_cost() + f.input.calc_cost(),
            Algebraic::Join(j) => Cost::new(2) + j.left.calc_cost() + j.right.calc_cost(),
            Algebraic::Union(u) => {
                Cost::new(u.inputs.len())
                    + u.inputs
                        .iter()
                        .fold(Cost::default(), |a, b| a * b.calc_cost())
            }
            Algebraic::Aggregate(a) => {
                Cost::new(1) + Cost::new(a.aggregates.len()) * a.input.calc_cost()
            }
            Algebraic::Variable(_) => Cost::new(1) + Cost::default(),
            Algebraic::Set(s) => s.set.iter().fold(Cost::default(), |a, b| {
                let b = b.calc_cost();
                if a < b {
                    a
                } else {
                    b
                }
            }),
            Algebraic::Sort(s) => {
                let cost = s.input.calc_cost();
                &cost * &cost
            }
        }
    }

    pub fn table(name: String) -> Algebraic {
        Algebraic::Scan(Scan::new(name))
    }

    pub fn project(project: Operator, input: Algebraic) -> Algebraic {
        Algebraic::Project(Project::new(project, input))
    }

    pub fn filter(condition: Operator, input: Algebraic) -> Algebraic {
        Algebraic::Filter(Filter::new(input, condition))
    }
}

impl InputDerivable for Algebraic {
    fn derive_input_layout(&self) -> Option<Layout> {
        match self {
            Algebraic::IndexScan(s) => s.derive_input_layout(),
            Algebraic::Project(p) => p.derive_input_layout(),
            Algebraic::Filter(f) => f.derive_input_layout(),
            Algebraic::Join(j) => j.derive_input_layout(),
            Algebraic::Union(u) => u.derive_input_layout(),
            Algebraic::Aggregate(a) => a.derive_input_layout(),
            Algebraic::Variable(v) => v.derive_input_layout(),
            Algebraic::Dual(d) => d.derive_input_layout(),
            Algebraic::Scan(t) => t.derive_input_layout(),
            Algebraic::Set(s) => s.derive_input_layout(),
            Algebraic::Sort(v) => v.input.derive_input_layout(),
        }
    }
}

impl OutputDerivable for Algebraic {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        match self {
            Algebraic::IndexScan(s) => s.derive_output_layout(inputs),
            Algebraic::Project(p) => p.derive_output_layout(inputs),
            Algebraic::Filter(f) => f.derive_output_layout(inputs),
            Algebraic::Join(j) => j.derive_output_layout(inputs),
            Algebraic::Union(u) => u.derive_output_layout(inputs),
            Algebraic::Aggregate(a) => a.derive_output_layout(inputs),
            Algebraic::Variable(v) => v.derive_output_layout(inputs),
            Algebraic::Dual(d) => d.derive_output_layout(inputs),
            Algebraic::Scan(t) => t.derive_output_layout(inputs),
            Algebraic::Set(s) => s.initial.derive_output_layout(inputs),
            Algebraic::Sort(s) => s.derive_output_layout(inputs),
        }
    }
}

impl Algebra for Algebraic {
    type Iterator = BoxedIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        match self {
            Algebraic::IndexScan(s) => Box::new(s.derive_iterator()),
            Algebraic::Project(p) => {
                let iter = Box::new(p.derive_iterator());
                match *iter {
                    ProjectIter::ValueProjectIterator(p) => Box::new(p),
                    ProjectIter::ValueSetProjectIterator(p) => Box::new(p),
                }
            }
            Algebraic::Filter(f) => Box::new(f.derive_iterator()),
            Algebraic::Join(j) => Box::new(j.derive_iterator()),
            Algebraic::Union(u) => Box::new(u.derive_iterator()),
            Algebraic::Aggregate(a) => Box::new(a.derive_iterator()),
            Algebraic::Variable(s) => Box::new(s.derive_iterator()),
            Algebraic::Dual(d) => Box::new(d.derive_iterator()),
            Algebraic::Scan(t) => Box::new(t.derive_iterator()),
            Algebraic::Set(s) => s.initial.derive_iterator(),
            Algebraic::Sort(s) => Box::new(s.derive_iterator()),
        }
    }
}

pub trait Algebra: Clone + InputDerivable + OutputDerivable {
    type Iterator: Iterator<Item = Value> + Send + 'static;
    fn derive_iterator(&mut self) -> Self::Iterator;
}

pub fn build_iterator(mut algebra: Algebraic) -> Result<BoxedIterator, String> {
    Ok(algebra.derive_iterator())
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
    fn get_storages(&self) -> Vec<ValueStore>;

    fn drain(&mut self) -> Vec<Value> {
        self.into_iter().collect()
    }

    fn drain_to_train(&mut self, stop: usize) -> Train {
        Train::new(self.drain()).mark(stop)
    }

    fn clone(&self) -> BoxedIterator;

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator>;
}
