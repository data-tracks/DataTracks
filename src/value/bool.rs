use std::fmt::Formatter;

use crate::value::{Float, Int, Text, ValType};
use crate::value::value::{Valuable};
use crate::value_display;

#[derive(Eq, Hash, Clone, Debug)]
pub struct Bool(pub bool);

impl Valuable for Bool {
    fn type_(&self) -> ValType {
        ValType::Bool
    }
}


impl PartialEq<&Int> for &Bool {
    fn eq(&self, other: &&Int) -> bool {
        other == self
    }
}

impl PartialEq<&Float> for &Bool {
    fn eq(&self, other: &&Float) -> bool {
        other == self
    }
}

impl PartialEq for Bool {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl PartialEq<Text> for Bool {
    fn eq(&self, other: &Text) -> bool {
        match other.0.parse::<bool>() {
            Ok(bo) => self.0 == bo,
            Err(_) => false
        }
    }
}

value_display!(Bool);