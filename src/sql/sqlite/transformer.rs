use crate::algebra::{BoxedIterator, ValueIterator};
use crate::analyse::Layoutable;
use crate::processing::option::Configurable;
use crate::processing::transform::{Transform, Transformer};
use crate::processing::{Layout, Train};
use crate::sql::sqlite::connection::SqliteConnector;
use crate::util::GLOBAL_ID;
use crate::value::Value;
use serde_json::Map;
use std::collections::HashMap;
use tokio::runtime::Runtime;

#[derive(Debug, PartialEq, Clone)]
pub struct LiteTransformer {
    id: i64,
    query: String,
    connector: SqliteConnector
}

impl LiteTransformer {
    fn new(query: String, path: String) -> LiteTransformer {
        let id = GLOBAL_ID.new_id();
        let connector = SqliteConnector::new(&path);
        LiteTransformer { id, connector, query }
    }
}

impl Configurable for LiteTransformer {
    fn get_name(&self) -> String {
        "SQLite".to_owned()
    }

    fn get_options(&self) -> Map<String, serde_json::Value> {
        let mut options = Map::new();
        self.connector.add_options(&mut options);
        options.insert(String::from("query"), serde_json::Value::String(self.query.clone()));
        options
    }
}

impl Layoutable for LiteTransformer {
    fn derive_input_layout(&self) -> Layout {
        todo!()
    }

    fn derive_output_layout(&self, _inputs: HashMap<String, &Layout>) -> Layout {
        todo!()
    }
}

impl Transformer for LiteTransformer {
    fn parse(options: Map<String, serde_json::Value>) -> Result<Self, String> {
        let query = options.get("query").unwrap().as_str().unwrap();
        let path = options.get("path").unwrap().as_str().unwrap();
        Ok(LiteTransformer::new(query.to_owned(), path.to_owned()))
    }

    fn optimize(&self, _transforms: HashMap<String, Transform>) -> Box<dyn ValueIterator<Item=Value> + Send> {
        let iter = LiteIterator::new(self.query.clone(), self.connector.path.clone(), self.connector.clone());

        Box::new(iter)
    }
}

pub struct LiteIterator {
    query: String,
    path: String,
    connector: SqliteConnector,
}

impl LiteIterator {
    pub fn new(query: String, path: String, connector: SqliteConnector) -> LiteIterator {
        LiteIterator { query, path, connector }
    }
}

impl Iterator for LiteIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        let runtime = Runtime::new().unwrap();
        let query = self.query.clone();
        let mut connection = self.connector.connect().unwrap();
        let values: Vec<Value> = runtime.block_on(async {
            let prepared = sqlx::query_as(&query);
            return prepared.fetch_all(&mut connection).await.unwrap();
        });
        if values.is_empty() {
            None
        } else {
            Some((*values.get(0).unwrap()).clone())
        }
    }
}

impl ValueIterator for LiteIterator {
    fn load(&mut self, _trains: Vec<Train>) {
        // nothing on purpose
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(LiteIterator::new(self.query.clone(), self.path.clone(), self.connector.clone()))
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}


#[cfg(test)]
mod tests {
    use crate::processing::Plan;

    const PLAN: &str = "\
            0--1{sql|SELECT id, name FROM $0, $lite($0.id)}\n\
            \n\
            Transform\n\
            $lite:SQLite{\"path\":\"memory:\",\"query\":\"SELECT id FROM company WHERE name = $\"}";

    #[test]
    fn test_simple_parse() {
        let plan = Plan::parse(PLAN).unwrap();
        assert_eq!(plan.dump().replace("\n", ""), PLAN.replace("\n", ""));
    }

    #[test]
    fn test_simple_operate() {
        let mut plan = Plan::parse(PLAN).unwrap();
        plan.operate()
    }
}