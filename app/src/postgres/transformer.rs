use crate::analyse::{InputDerivable, OutputDerivationStrategy};
use crate::language::Language;
use crate::postgres::connection::PostgresConnection;
use crate::postgres::util::PostgresIterator;
use crate::processing::Layout;
use crate::processing::transform::Transformer;
use crate::util::DynamicQuery;
use core::BoxedValueIterator;
use core::Configurable;
use serde_json::{Map, Value};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
pub struct PostgresTransformer {
    pub(crate) connector: PostgresConnection,
    pub(crate) query: DynamicQuery,
    output_derivation_strategy: OutputDerivationStrategy,
}

impl PostgresTransformer {
    pub fn new(url: String, port: u16, db: String, query: String, user: String) -> Self {
        let query = DynamicQuery::build_dynamic_query(query.clone());
        let connector = PostgresConnection::new(url, port, db, user);
        let output_derivation_strategy =
            OutputDerivationStrategy::query_based(query.get_query(), Language::Sql)
                .unwrap_or_default();
        PostgresTransformer {
            connector,
            query,
            output_derivation_strategy,
        }
    }
}

impl Configurable for PostgresTransformer {
    fn name(&self) -> String {
        "Postgres".to_owned()
    }

    fn options(&self) -> Map<String, Value> {
        let mut options = Map::new();
        self.connector.add_options(&mut options);
        options.insert(String::from("query"), Value::String(self.query.get_query()));
        options
    }
}

impl InputDerivable for PostgresTransformer {
    fn derive_input_layout(&self) -> Option<Layout> {
        Some(self.query.derive_input_layout())
    }
}

fn error(param: &str) -> String {
    format!("Missing {param} parameter")
}

impl Transformer for PostgresTransformer {
    fn parse(options: Map<String, Value>) -> Result<Self, String> {
        let query = options
            .get("query")
            .and_then(Value::as_str)
            .ok_or(error("query"))?
            .to_string();
        let url = options
            .get("url")
            .and_then(Value::as_str)
            .ok_or(error("url"))?
            .to_string();
        let port = options
            .get("port")
            .and_then(Value::as_i64)
            .ok_or(error("port"))?
            .to_string()
            .parse::<u16>()
            .map_err(|e| e.to_string())?;
        let db = options
            .get("database")
            .and_then(Value::as_str)
            .ok_or(error("database name"))?
            .to_string();
        let user = options
            .get("user")
            .and_then(Value::as_str)
            .ok_or(error("user name"))?
            .to_string();
        Ok(PostgresTransformer::new(url, port, db, query, user))
    }

    fn optimize(&self, _transforms: HashMap<String, BoxedValueIterator>) -> BoxedValueIterator {
        Box::new(PostgresIterator::new(
            self.query.clone(),
            self.connector.clone(),
        ).unwrap())
    }

    fn get_output_derivation_strategy(&self) -> &OutputDerivationStrategy {
        &self.output_derivation_strategy
    }
}
