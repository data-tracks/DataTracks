use crate::value_display;
use crate::{Bool, Float, Int};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::cmp::PartialEq;
use std::fmt::Formatter;

#[derive(
    Eq, Hash, Debug, PartialEq, Clone, Serialize, Deserialize, Ord, PartialOrd, Readable, Writable,
)]
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
