use crate::algebra::{Algebra, AlgebraType};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::Layout;
use crate::util::EmptyIterator;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct AlgSet {
    pub initial: Box<AlgebraType>,
    pub set: Vec<AlgebraType>,
}

impl AlgSet {
    pub fn new(initial: AlgebraType) -> AlgSet {
        AlgSet { initial: Box::new(initial), set: vec![] }
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