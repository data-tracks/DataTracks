use std::fmt::Display;

use crate::language::Language::{Mql, Sql};

#[derive(Clone, Debug, PartialEq)]
pub enum Language {
    Sql,
    Mql,
}

impl Language {
    pub(crate) fn name(&self) -> String {
        match self {
            Sql => "sql".to_string(),
            Mql => "mql".to_string(),
        }
    }
}

impl Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Sql => write!(f, "sql"),
            Mql => write!(f, "mql"),
        }
    }
}

impl TryFrom<&str> for Language {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "sql" => Ok(Sql),
            "mql" => Ok(Mql),
            _ => Err(format!("invalid language: {value}")),
        }
    }
}
