use std::fmt::{Display, Formatter};
use crate::value::ValType;
use crate::value::value::{Valuable};

#[derive(Eq, Hash, Clone, Debug, PartialEq)]
pub struct Null {}

impl Valuable for Null {
    fn type_(&self) -> ValType {
        ValType::Null
    }
}

impl Display for Null {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "null")
    }
}