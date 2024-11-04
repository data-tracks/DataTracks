use crate::algebra::{BoxedIterator, ValueIterator};
use crate::analyse::Layoutable;
use crate::processing::option::Configurable;
use crate::processing::transform::{Transform, Transformer};
use crate::processing::{Layout, Train};
use crate::sql::postgres::connection::PostgresConnection;
use crate::util::{DynamicQuery, ReplaceType, Segment, ValueExtractor};
use crate::value::value;
use postgres::{Client, Statement};
use serde_json::{Map, Value};
use std::collections::HashMap;
use tokio::runtime::Runtime;

#[derive(Clone, Debug, PartialEq)]
pub struct PostgresTransformer {
    connector: PostgresConnection,
    query: DynamicQuery,
}

impl PostgresTransformer {
    pub fn new(url: String, port: u16, db: String, query: String) -> PostgresTransformer {
        let query = DynamicQuery::build_dynamic_query(query.clone());
        let connector = PostgresConnection::new(url, port, db);
        PostgresTransformer { connector, query }
    }
}

impl Configurable for PostgresTransformer {
    fn get_name(&self) -> String {
        "Postgres".to_owned()
    }

    fn get_options(&self) -> Map<String, Value> {
        let mut options = Map::new();
        self.connector.add_options(&mut options);
        options.insert(String::from("query"), Value::String(self.query.get_query()));
        options
    }
}

impl Layoutable for PostgresTransformer {
    fn derive_input_layout(&self) -> Layout {
        match self.query.get_replacement_type() {
            ReplaceType::Key => {
                let mut keys = vec![];
                for part in self.query.get_parts() {
                    if let Segment::DynamicKey(key) = part {
                        keys.push(key.clone());
                    }
                }
                Layout::dict(keys)
            }
            ReplaceType::Index => {
                let mut indexes = vec![];
                for part in self.query.get_parts() {
                    if let Segment::DynamicIndex(index) = part {
                        indexes.push(index.clone());
                    }
                }
                indexes.iter().max().map(|i| Layout::array(Some(*i as i32))).unwrap_or(Layout::array(None))
            }
            ReplaceType::Full => {
                Layout::default()
            }
        }
    }

    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Layout {
        todo!()
    }
}

impl Transformer for PostgresTransformer {
    fn parse(options: Map<String, Value>) -> Result<Self, String> {
        let query = options.get("query").unwrap().as_str().unwrap();
        let url = options.get("url").unwrap().as_str().unwrap();
        let port = options.get("port").unwrap().as_i64().unwrap();
        let db = options.get("db").unwrap().as_str().unwrap();
        Ok(PostgresTransformer::new(url.to_owned(), port.to_owned() as u16, db.to_owned(), query.to_owned()))
    }

    fn optimize(&self, _transforms: HashMap<String, Transform>) -> Box<dyn ValueIterator<Item=value::Value> + Send> {
        Box::new(PostgresIterator::new(self.query.clone(), self.connector.clone()))
    }
}

pub struct PostgresIterator {
    client: Client,
    query: DynamicQuery,
    connector: PostgresConnection,
    statement: Statement,
    value_functions: ValueExtractor,
    values: Vec<value::Value>,
}


impl PostgresIterator {
    pub fn new(query: DynamicQuery, connector: PostgresConnection) -> Self {
        let (q, value_functions) = query.prepare_query("$", None);
        let con = connector.clone();
        let statement = Runtime::new().unwrap().block_on(async {
            let mut client = con.connect().await.unwrap();

            return client.prepare(&q).unwrap();
        });

        let client = Runtime::new().unwrap().block_on(async {
            connector.clone().connect().await.unwrap()
        });

        PostgresIterator { client, query, connector, statement, value_functions, values: vec![] }
    }

    pub(crate) async fn query_values(&mut self, value: value::Value) -> Vec<value::Value> {
        let values = (self.value_functions)(&value).iter().collect();
        self.client.query(&self.statement, &values).unwrap().iter().map(|v| v.into()).collect()
    }
}

impl Iterator for PostgresIterator {
    type Item = value::Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() {
            None
        } else {
            Some(self.values.remove(0))
        }
    }
}

impl ValueIterator for PostgresIterator {
    fn dynamically_load(&mut self, trains: Vec<Train>) {
        for train in trains {
            if let Some(values) = train.values {
                for value in values {
                    self.values.append(&mut self.query_values(value));
                }
            }
        }
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(PostgresIterator::new(self.query.clone(), self.connector.clone()))
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}