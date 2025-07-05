use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::{Algebra, Algebraic};
use crate::optimize::Rule;
use crate::processing::Layout;
use crate::util::EmptyIterator;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AlgSet {
    id: usize,
    pub initial: Box<Algebraic>,
    pub rules: Vec<Rule>,
}

impl AlgSet {
    pub fn new(id: usize, initial: Algebraic) -> AlgSet {
        AlgSet {
            id,
            initial: Box::new(initial),
            rules: vec![],
        }
    }
}


impl Hash for AlgSet {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.initial.hash(state);
    }
}

impl AlgInputDerivable for AlgSet {
    fn derive_input_layout(&self, root: &AlgebraRoot) -> Option<Layout> {
        self.initial.derive_input_layout(root)
    }
}

impl AlgOutputDerivable for AlgSet {
    fn derive_output_layout(&self, inputs: HashMap<String, Layout>, root: &AlgebraRoot) -> Option<Layout> {
        self.initial.derive_output_layout(inputs, root)
    }
}

impl Algebra for AlgSet {
    type Iterator = EmptyIterator;

    fn id(&self) -> usize {
        self.initial.id()
    }

    fn replace_id(self, _id: usize) -> Self {
        self
    }

    fn derive_iterator(&self, _root: &AlgebraRoot) -> Result<Self::Iterator, String> {
        Err(String::from("Algebra not implemented"))
    }
}
