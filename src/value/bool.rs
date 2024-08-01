use std::fmt::Formatter;

use crate::value::{Float, Int, Text};
use crate::value_display;

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub struct Bool(pub bool);


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


impl PartialEq<Text> for Bool {
    fn eq(&self, other: &Text) -> bool {
        match other.0.parse::<bool>() {
            Ok(bo) => self.0 == bo,
            Err(_) => false
        }
    }
}

value_display!(Bool);