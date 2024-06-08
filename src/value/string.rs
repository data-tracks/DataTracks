use std::fmt::{Display, Formatter};

use crate::value::value::{ValType, Valuable};
use crate::value::value::ValType::String as StringType;

#[derive(Debug, PartialEq)]
pub struct HoString(pub String);

impl Valuable for HoString{
    fn type_(&self) -> ValType {
        return StringType
    }
}

impl Display for HoString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
