use std::fmt::{Display};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub enum RelationalType {
    Varchar(u64),
    Integer,
    Float,
    Bool,
    Text,
}

impl Display for RelationalType {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            RelationalType::Varchar(num) => fmt.write_fmt(format_args!("VARCHAR({})", num)),
            RelationalType::Integer => fmt.write_str("INTEGER"),
            RelationalType::Float => fmt.write_str("FLOAT"),
            RelationalType::Bool => fmt.write_str("BOOLEAN"),
            RelationalType::Text => fmt.write_str("TEXT"),
        }
    }
}