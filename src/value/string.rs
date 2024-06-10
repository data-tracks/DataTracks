use std::fmt::Formatter;
use crate::value::{HoBool, HoFloat, HoInt};

use crate::value::value::{ValType, Valuable};
use crate::value::value::ValType::Text;
use crate::value::value::value_display;

#[derive(Debug, PartialEq, Clone)]
pub struct HoString(pub String);

impl Valuable for HoString{
    fn type_(&self) -> ValType {
        return Text
    }
}

impl PartialEq<HoInt> for Box<HoString> {
    fn eq(&self, other: &HoInt) -> bool {
        other == self
    }
}

impl PartialEq<HoFloat> for Box<HoString> {
    fn eq(&self, other: &HoFloat) -> bool {
        other == self
    }
}

impl PartialEq<HoBool> for Box<HoString> {
    fn eq(&self, other: &HoBool) -> bool {
        other == self
    }
}

value_display!(HoString);
