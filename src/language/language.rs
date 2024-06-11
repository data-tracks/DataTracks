use crate::language::Language::{MQL, SQL};

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