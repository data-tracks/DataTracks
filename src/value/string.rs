use std::fmt::Formatter;

use crate::value::value::{ValType, Valuable};
use crate::value::value::ValType::String as StringType;
use crate::value::value::value_display;

#[derive(Debug, PartialEq)]
pub struct HoString(pub String);

impl Valuable for HoString{
    fn type_(&self) -> ValType {
        return StringType
    }
}

value_display!(HoString);
