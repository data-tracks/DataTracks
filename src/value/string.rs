use std::cmp::PartialEq;
use std::fmt::Formatter;

use crate::value::{Bool, Float, Int};
use crate::value_display;

#[derive(Eq, Hash, Debug, PartialEq, Clone)]
pub struct Text(pub String);

impl PartialEq<Int> for Text {
    fn eq(&self, other: &Int) -> bool {
        other == self
    }
}

impl PartialEq<Float> for Text {
    fn eq(&self, other: &Float) -> bool {
        other == self
    }
}

impl PartialEq<Bool> for Text {
    fn eq(&self, other: &Bool) -> bool {
        other == self
    }
}

value_display!(Text);
