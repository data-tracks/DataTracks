use std::fmt::{Display, Formatter};

use crate::value::value::{ValType, Valuable};

#[derive(Eq, Hash, Clone, Debug, PartialEq)]
pub struct HoNull {}

impl Valuable for HoNull {
    fn type_(&self) -> ValType {
        ValType::Null
    }
}

impl Display for HoNull {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "null")
    }
}