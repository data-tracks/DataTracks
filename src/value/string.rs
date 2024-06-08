use std::fmt;
use std::fmt::Formatter;

use crate::value::value::{ValType, Valuable};
use crate::value::value::ValType::String;

#[derive(Debug, PartialEq)]
pub struct HoString(pub str);

impl Valuable for HoString{
    fn type_(&self) -> ValType {
        return String
    }
}

impl fmt::Display for HoString{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}