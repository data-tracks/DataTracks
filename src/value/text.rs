use crate::value::{Bool, Float, Int};
use crate::value_display;
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::fmt::Formatter;
use speedy::{Readable, Writable};

#[derive(Eq, Hash, Debug, PartialEq, Clone, Serialize, Deserialize, Ord, PartialOrd, Readable, Writable)]
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
