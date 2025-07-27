use crate::algebra::aggregate::{Aggregate, ValueLoader};
use crate::algebra::dual::Dual;
use crate::algebra::filter::Filter;
use crate::algebra::join::Join;
use crate::algebra::project::Project;
use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::scan::IndexScan;
use crate::algebra::sort::Sort;
use crate::algebra::union::Union;
use crate::algebra::variable::VariableScan;
use crate::algebra::Scan;
use crate::optimize::Cost;
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
use crate::util::reservoir::ValueReservoir;
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
    pub(crate) fn calc_cost(&self, root: &AlgebraRoot) -> Cost {
        match self {
            Algebraic::Dual(_) => Cost::new(1),
            Algebraic::IndexScan(_) => Cost::new(1),
            Algebraic::Scan(_) => Cost::new(1),
            Algebraic::Project(p) => {
                Cost::new(1)
                    + p.project.calc_cost()
                    + root.get_child(p.id()).unwrap().calc_cost(root)
            }
            Algebraic::Filter(f) => {
                Cost::new(1)
                    + f.condition.calc_cost()
                    + root.get_child(f.id()).unwrap().calc_cost(root)
            }
            Algebraic::Join(j) => {
                let children = root.get_children(j.id());
                Cost::new(2)
                    + children.get(0).unwrap().calc_cost(root)
                    + children.get(1).unwrap().calc_cost(root)
            }
            Algebraic::Union(u) => {
                let children = root.get_children(u.id());
                Cost::new(children.len())
                    + children
                        .iter()
                        .fold(Cost::default(), |a, b| a * b.calc_cost(root))
            }
            Algebraic::Aggregate(a) => {
                Cost::new(1)
                    + Cost::new(a.aggregates.len())
                        * root.get_child(a.id()).unwrap().calc_cost(root)
            }
            Algebraic::Variable(_) => Cost::new(1) + Cost::default(),
            Algebraic::Sort(s) => {
                let cost = root.get_child(s.id()).unwrap().calc_cost(root);
                &cost * &cost
            }
        }
    }
}

impl AlgInputDerivable for Algebraic {
    fn derive_input_layout(&self, root: &AlgebraRoot) -> Option<Layout> {
        match self {
            Algebraic::IndexScan(s) => s.derive_input_layout(root),
            Algebraic::Project(p) => p.derive_input_layout(root),
            Algebraic::Filter(f) => f.derive_input_layout(root),
            Algebraic::Join(j) => j.derive_input_layout(root),
            Algebraic::Union(u) => u.derive_input_layout(root),
            Algebraic::Aggregate(a) => a.derive_input_layout(root),
            Algebraic::Variable(v) => v.derive_input_layout(root),
            Algebraic::Dual(d) => d.derive_input_layout(root),
            Algebraic::Scan(t) => t.derive_input_layout(root),
            Algebraic::Sort(v) => root.get_child(v.id())?.derive_input_layout(root),
        }
    }
}

impl AlgOutputDerivable for Algebraic {
    fn derive_output_layout(
        &self,
        inputs: HashMap<String, Layout>,
        root: &AlgebraRoot,
    ) -> Option<Layout> {
        match self {
            Algebraic::IndexScan(s) => s.derive_output_layout(inputs, root),
            Algebraic::Project(p) => p.derive_output_layout(inputs, root),
            Algebraic::Filter(f) => f.derive_output_layout(inputs, root),
            Algebraic::Join(j) => j.derive_output_layout(inputs, root),
            Algebraic::Union(u) => u.derive_output_layout(inputs, root),
            Algebraic::Aggregate(a) => a.derive_output_layout(inputs, root),
            Algebraic::Variable(v) => v.derive_output_layout(inputs, root),
            Algebraic::Dual(d) => d.derive_output_layout(inputs, root),
            Algebraic::Scan(t) => t.derive_output_layout(inputs, root),
            Algebraic::Sort(s) => s.derive_output_layout(inputs, root),
        }
    }
}

impl Algebra for Algebraic {
    type Iterator = BoxedIterator;

    fn id(&self) -> usize {
        match self {
            Algebraic::Dual(d) => d.id(),
            Algebraic::IndexScan(s) => s.id(),
            Algebraic::Scan(s) => s.id(),
            Algebraic::Project(p) => p.id(),
            Algebraic::Filter(f) => f.id(),
            Algebraic::Join(j) => j.id(),
            Algebraic::Union(u) => u.id(),
            Algebraic::Aggregate(a) => a.id(),
            Algebraic::Variable(v) => v.id(),
            Algebraic::Sort(s) => s.id(),
        }
    }

    fn replace_id(self, id: usize) -> Self {
        match self {
            Algebraic::Dual(d) => Algebraic::Dual(d.replace_id(id)),
            Algebraic::IndexScan(i) => Algebraic::IndexScan(i.replace_id(id)),
            Algebraic::Scan(s) => Algebraic::Scan(s.replace_id(id)),
            Algebraic::Project(p) => Algebraic::Project(p.replace_id(id)),
            Algebraic::Filter(f) => Algebraic::Filter(f.replace_id(id)),
            Algebraic::Join(j) => Algebraic::Join(j.replace_id(id)),
            Algebraic::Union(u) => Algebraic::Union(u.replace_id(id)),
            Algebraic::Aggregate(a) => Algebraic::Aggregate(a.replace_id(id)),
            Algebraic::Variable(v) => Algebraic::Variable(v.replace_id(id)),
            Algebraic::Sort(s) => Algebraic::Sort(s.replace_id(id)),
        }
    }

    fn derive_iterator(&self, root: &AlgebraRoot) -> Result<Self::Iterator, String> {
        Ok(match self {
            Algebraic::IndexScan(s) => Box::new(s.derive_iterator(root)?),
            Algebraic::Project(p) => Box::new(p.derive_iterator(root)?),
            Algebraic::Filter(f) => Box::new(f.derive_iterator(root)?),
            Algebraic::Join(j) => Box::new(j.derive_iterator(root)?),
            Algebraic::Union(u) => Box::new(u.derive_iterator(root)?),
            Algebraic::Aggregate(a) => Box::new(a.derive_iterator(root)?),
            Algebraic::Variable(s) => Box::new(s.derive_iterator(root)?),
            Algebraic::Dual(d) => Box::new(d.derive_iterator(root)?),
            Algebraic::Scan(t) => Box::new(t.derive_iterator(root)?),
            Algebraic::Sort(s) => Box::new(s.derive_iterator(root)?),
        })
    }
}

pub trait Algebra: Clone + AlgInputDerivable + AlgOutputDerivable {
    type Iterator: Iterator<Item = Value> + Send + 'static;
    fn id(&self) -> usize;
    fn replace_id(self, id: usize) -> Self;
    fn derive_iterator(&self, root: &AlgebraRoot) -> Result<Self::Iterator, String>;
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
    fn get_storages(&self) -> Vec<ValueReservoir>;

    fn drain(&mut self) -> Vec<Value> {
        self.into_iter().collect()
    }

    fn drain_to_train(&mut self, stop: usize) -> Train {
        Train::new(self.drain(), 0).mark(stop)
    }

    fn clone(&self) -> BoxedIterator;

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator>;
}
