
use std::collections::HashMap;
use crate::processing::Layout;

pub trait Layoutable {
    fn derive_input_layout(&self) -> Layout;

    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Layout;
}