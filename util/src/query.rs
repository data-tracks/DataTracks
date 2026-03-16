use processing::{Algebra, parse_sql};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Query {
    #[serde(alias = "sql")]
    SQL(String),
    #[serde(alias = "mql")]
    MQL(String),
    #[serde(alias = "cypher")]
    Cypher(String),
}

impl From<Query> for Algebra {
    fn from(value: Query) -> Self {
        match value {
            Query::SQL(s) => parse_sql(&s),
            Query::MQL(m) => Algebra::T(m.clone()),
            Query::Cypher(c) => Algebra::T(c.clone()),
        }
    }
}
