use crate::algebra::{AlgebraType, ValueIterator};
use crate::analyse::Layoutable;
use crate::processing::option::Configurable;
use crate::processing::transform::{Transform, Transformer};
use crate::processing::Layout;
use crate::value::Value;
use serde_json::Map;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub struct LiteTransformer {
    id: i64,
    query: String,
    algebra: AlgebraType
}

impl Clone for LiteTransformer {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl Configurable for LiteTransformer {
    fn get_name(&self) -> String {
        todo!()
    }

    fn get_options(&self) -> Map<String, serde_json::Value> {
        todo!()
    }
}

impl Layoutable for LiteTransformer {
    fn derive_input_layout(&self) -> Layout {
        todo!()
    }

    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Layout {
        todo!()
    }
}

impl Transformer for LiteTransformer {

    fn parse(&self, stencil: &str) -> Result<Transform, String> {
        todo!()
    }

    fn optimize(&self, transforms: HashMap<String, Transform>) -> Box<dyn ValueIterator<Item=Value> + Send> {
        todo!()
    }
}