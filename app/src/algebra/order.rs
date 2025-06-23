use crate::algebra::implement::implement;
use crate::algebra::{BoxedValueHandler, Operator};

#[derive(Debug, Clone, Default, Eq, Hash, PartialEq)]
pub enum Order {
    #[default]
    None,
    Field(Operator, Direction),
}

impl Order {
    pub fn derive_handler(&self) -> Option<(BoxedValueHandler, Direction)> {
        match self {
            Order::None => None,
            Order::Field(op, dir) => Some((implement(op), dir.clone())),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Direction {
    Asc,
    Desc,
}
