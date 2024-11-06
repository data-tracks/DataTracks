use crate::processing::Layout;
use std::collections::HashMap;


pub trait InputDerivable {
    fn derive_input_layout(&self) -> Layout;
}

pub trait OutputDerivable {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Layout;
}


pub enum OutputDerivationStrategy {}

impl OutputDerivationStrategy {}

impl OutputDerivable for OutputDerivationStrategy {
    fn derive_output_layout(&self, _inputs: HashMap<String, &Layout>) -> Layout {
        todo!()
    }
}