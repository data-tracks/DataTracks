use crate::algebra::{BoxedIterator, ValueIterator};
use crate::analyse::{InputDerivable, OutputDerivationStrategy};
use crate::language::Language;
use crate::processing::option::Configurable;
use crate::processing::transform::{Transform, Transformer};
use crate::processing::Layout;
use crate::sql::postgres::connection::PostgresConnection;
use crate::util::reservoir::ValueReservoir;
use crate::util::{DynamicQuery, ValueExtractor};
use postgres::types::ToSql;
use postgres::{Client, Statement};
use serde_json::{Map, Value};
use std::collections::HashMap;
use value;

#[derive(Clone, Debug, PartialEq)]
pub struct PostgresTransformer {
    pub(crate) connector: PostgresConnection,
    pub(crate) query: DynamicQuery,
    output_derivation_strategy: OutputDerivationStrategy,
}

impl PostgresTransformer {
    pub fn new(url: String, port: u16, db: String, query: String) -> PostgresTransformer {
        let query = DynamicQuery::build_dynamic_query(query.clone());
        let connector = PostgresConnection::new(url, port, db);
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
    format!("Missing {} parameter", param)
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
            .get("db")
            .and_then(Value::as_str)
            .ok_or(error("database name"))?
            .to_string();
        Ok(PostgresTransformer::new(url, port, db, query))
    }

    fn optimize(
        &self,
        _transforms: HashMap<String, Transform>,
    ) -> Box<dyn ValueIterator<Item = value::Value> + Send> {
        Box::new(PostgresIterator::new(
            self.query.clone(),
            self.connector.clone(),
        ))
    }

    fn get_output_derivation_strategy(&self) -> &OutputDerivationStrategy {
        &self.output_derivation_strategy
    }
}

pub struct PostgresIterator {
    client: Client,
    query: DynamicQuery,
    connector: PostgresConnection,
    statement: Statement,
    value_functions: ValueExtractor,
    values: Vec<value::Value>,
    storage: ValueReservoir,
}

impl PostgresIterator {
    pub fn new(query: DynamicQuery, connector: PostgresConnection) -> Self {
        let (q, value_functions) = query.prepare_query("$", None);
        let con = connector.clone();

        let mut client = con.connect().unwrap();

        let statement = client.prepare(&q).unwrap();

        let client = connector.clone().connect().unwrap();

        PostgresIterator {
            client,
            query,
            connector,
            statement,
            value_functions,
            values: vec![],
            storage: Default::default(),
        }
    }

    fn load(&mut self) {
        for value in self.storage.drain() {
            let values = &mut self.query_values(value);
            self.values.append(values);
        }
    }

    pub(crate) fn query_values(&mut self, value: value::Value) -> Vec<value::Value> {
        let values = (self.value_functions)(&value);
        let values = values
            .iter()
            .map(|v| v as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        let values: &[&(dyn ToSql + Sync)] = &values;
        self.client
            .query(&self.statement, values)
            .unwrap()
            .into_iter()
            .map(|v| v.into())
            .collect()
    }
}

impl Iterator for PostgresIterator {
    type Item = value::Value;

    fn next(&mut self) -> Option<Self::Item> {
        self.load();
        if self.values.is_empty() {
            None
        } else {
            Some(self.values.remove(0))
        }
    }
}

impl ValueIterator for PostgresIterator {
    fn get_storages(&self) -> Vec<ValueReservoir> {
        vec![self.storage.clone()]
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(PostgresIterator::new(
            self.query.clone(),
            self.connector.clone(),
        ))
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}
