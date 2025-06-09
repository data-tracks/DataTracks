use std::fmt::{Display, Formatter};

#[derive(Eq, Hash, Clone, Debug, PartialEq)]
pub struct Null {}

impl Display for Null {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "null")
    }
}
