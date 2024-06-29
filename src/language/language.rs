use crate::algebra::AlgebraType;
use crate::language::{mql, sql};
use crate::language::Language::{MQL, SQL};

#[derive(Clone)]
pub enum Language {
    SQL,
    MQL,
}

impl Language {
    pub(crate) fn name(&self) -> String {
        match self {
            SQL => "sql".to_string(),
            MQL => "mql".to_string()
        }
    }

    fn parse(&self, query: &str) -> Result<AlgebraType, String> {
        match self {
            SQL => sql::transform(query),
            MQL => mql::transform(query),
            _ => Err("Language not supported.".to_string())
        }
    }
}

impl TryFrom<&str> for Language {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "sql" => Ok(SQL),
            "mql" => Ok(MQL),
            _ => Err(format!("invalid language: {}", value))
        }
    }
}