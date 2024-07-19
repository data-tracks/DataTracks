use crate::algebra::AlgebraType;
use crate::language::{mql, sql};
use crate::language::Language::{Mql, Sql};

#[derive(Clone)]
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