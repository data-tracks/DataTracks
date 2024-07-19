use std::fmt::Formatter;

use crate::value::{HoFloat, HoInt, HoString};
use crate::value::value::{ValType, Valuable};
use crate::value::value::value_display;

#[derive(Eq, Hash, Clone, Debug)]
pub struct HoBool(pub bool);

impl Valuable for HoBool {
    fn type_(&self) -> ValType {
        ValType::Bool
    }
}


impl PartialEq<&HoInt> for &HoBool {
    fn eq(&self, other: &&HoInt) -> bool {
        other == self
    }
}

impl PartialEq<&HoFloat> for &HoBool {
    fn eq(&self, other: &&HoFloat) -> bool {
        other == self
    }
}

impl PartialEq for HoBool {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl PartialEq<Box<HoString>> for HoBool {
    fn eq(&self, other: &Box<HoString>) -> bool {
        match other.0.parse::<bool>() {
            Ok(bo) => self.0 == bo,
            Err(_) => false
        }
    }
}

value_display!(HoBool);