use crate::algebra::{Algebra, AlgebraType};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::Layout;
use crate::util::EmptyIterator;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use crate::optimize::Rule;

#[derive(Debug, Clone)]
pub struct AlgSet {
    pub initial: Box<AlgebraType>,
    pub rules: Vec<Rule>,
    pub set: HashSet<AlgebraType>,
}

impl AlgSet {
    pub fn new(initial: AlgebraType) -> AlgSet {
        let set = HashSet::from_iter(vec![initial.clone()]);
        AlgSet { initial: Box::new(initial), set, rules: vec![] }
    }
}

impl PartialEq<Self> for AlgSet {
    fn eq(&self, other: &Self) -> bool {
        self.set.eq(&other.set) && self.rules.eq(&other.rules) && self.initial.eq(&other.initial)
    }
}

impl Eq for AlgSet {}

impl Hash for AlgSet {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.initial.hash(state);
    }
}

impl InputDerivable for AlgSet {
    fn derive_input_layout(&self) -> Option<Layout> {
        self.initial.derive_input_layout()
    }
}

impl OutputDerivable for AlgSet {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        self.initial.derive_output_layout(inputs)
    }
}

impl Algebra for AlgSet {
    type Iterator = EmptyIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        panic!("Algebra not implemented");
    }
}