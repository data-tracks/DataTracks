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

