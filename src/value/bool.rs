use std::fmt::Formatter;

use crate::value::value::{ValType, Valuable};
use crate::value::value::value_display;

pub struct HoBool(pub bool);

impl Valuable for HoBool {
    fn type_(&self) -> ValType {
        return ValType::Bool;
    }
}

value_display!(HoBool);