use crate::language::Language::{MQL, SQL};
use crate::language::sql::sql;
use crate::language::statement::Statement;

pub trait Languagable {
    fn parse(&self, query: &str) -> Result<Box<dyn Statement>, String>;
}

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
}

impl Languagable for Language {
    fn parse(&self, query: &str) -> Result<Box<dyn Statement>, String> {
        match self {
            SQL => sql::transform(query),
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