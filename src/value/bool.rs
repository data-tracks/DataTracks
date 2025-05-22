use crate::value::{Float, Int, Text};
use crate::value_display;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use schemas::message_generated::protocol::{Bool as FlatBool, BoolArgs};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::fmt::Formatter;

#[derive(Eq, PartialEq, Hash, Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Readable, Writable)]
pub struct Bool(pub bool);


impl Bool {
    pub(crate) fn new(bool: bool) -> Bool {
        Bool(bool)
    }

    pub(crate) fn flatternize<'bldr>(&self, builder: &mut FlatBufferBuilder<'bldr>) -> WIPOffset<FlatBool<'bldr>> {
        FlatBool::create(builder, &BoolArgs{ data: self.0 })
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


impl PartialEq<Text> for Bool {
    fn eq(&self, other: &Text) -> bool {
        match other.0.parse::<bool>() {
            Ok(bo) => self.0 == bo,
            Err(_) => false
        }
    }
}

value_display!(Bool);