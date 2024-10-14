use std::fmt::Display;

use crate::algebra::AlgebraType;
use crate::language::Language::{Mql, Sql};
use crate::language::{mql, sql};

#[derive(Clone, Debug, PartialEq)]
pub enum Language {
    Sql,
    Mql,
}

impl Language {
    pub(crate) fn name(&self) -> String {
        match self {
            Sql => "sql".to_string(),
            Mql => "mql".to_string()
        }
    }

    fn parse(&self, query: &str) -> Result<AlgebraType, String> {
        match self {
            Sql => sql::transform(query),
            Mql => mql::transform(query),
        }
    }
}

impl Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Sql => write!(f, "sql"),
            Mql => write!(f, "mql")
        }
    }
}


impl TryFrom<&str> for Language {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "sql" => Ok(Sql),
            "mql" => Ok(Mql),
            _ => Err(format!("invalid language: {}", value))
        }
    }
}