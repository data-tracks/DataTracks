use crate::algebra::AlgebraType;
use crate::processing::transform::{Transform, Transformer};

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

impl Transformer for LiteTransformer {
    fn dump(&self) -> String {
        todo!()
    }

    fn parse(&self, stencil: &str) -> Result<Transform, String> {
        todo!()
    }

    fn get_name(&self) -> String {
        "SQLite".to_string()
    }
}