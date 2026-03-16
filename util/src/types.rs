use serde::{Deserialize, Serialize};
use std::fmt::Display;
use value::ValType;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RelationalType {
    #[serde(alias = "varchar", alias = "VARCHAR")]
    Varchar(u64),
    #[serde(
        alias = "int",
        alias = "Int",
        alias = "INT",
        alias = "INTEGER",
        alias = "integer"
    )]
    Integer,
    #[serde(alias = "float", alias = "FLOAT")]
    Float,
    Bool,
    #[serde(alias = "string", alias = "text", alias = "TEXT")]
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

impl Into<ValType> for RelationalType {
    fn into(self) -> ValType {
        match self {
            RelationalType::Varchar(_) => ValType::Text,
            RelationalType::Integer => ValType::Integer,
            RelationalType::Float => ValType::Float,
            RelationalType::Bool => ValType::Bool,
            RelationalType::Text => ValType::Text,
        }
    }
}

impl From<&ValType> for RelationalType {
    fn from(t: &ValType) -> Self {
        match t {
            ValType::Integer => RelationalType::Integer,
            ValType::Float => RelationalType::Float,
            ValType::Text => RelationalType::Text,
            ValType::Bool => RelationalType::Bool,
            _ => todo!(),
        }
    }
}
