use crate::algebra::{BoxedIterator, ValueIterator};
use crate::analyse::Layoutable;
use crate::processing::option::Configurable;
use crate::processing::transform::{Transform, Transformer};
use crate::processing::{Layout, Train};
use crate::sql::sqlite::connection::SqliteConnector;
use crate::util::{DynamicQuery, GLOBAL_ID};
use crate::value::Value;
use serde_json::Map;
use std::collections::HashMap;
use tokio::runtime::Runtime;

#[derive(Debug, PartialEq, Clone)]
pub struct LiteTransformer {
    id: i64,
    query: DynamicQuery,
    connector: SqliteConnector
}

impl LiteTransformer {
    fn new(query: String, path: String) -> LiteTransformer {
        let id = GLOBAL_ID.new_id();
        let connector = SqliteConnector::new(&path);
        let query = DynamicQuery::build_dynamic_query(query);
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
        options.insert(String::from("query"), serde_json::Value::String(self.query.get_query()));
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
    query: DynamicQuery,
    path: String,
    connector: SqliteConnector,
    values: Vec<Value>
}

impl LiteIterator {
    pub fn new(query: DynamicQuery, path: String, connector: SqliteConnector) -> LiteIterator {
        LiteIterator { query, path, connector, values: Vec::new() }
    }

    fn query_values(&self, value: Value) -> Vec<Value> {
        let runtime = Runtime::new().unwrap();
        let query = self.query.clone();
        let mut connection = self.connector.connect().unwrap();
        runtime.block_on(async {
            let (query, value_function) = query.prepare_query("$");
            let mut prepared = sqlx::query_as(&query);
            for value in value_function(&value) {
                prepared = prepared.bind(value)
            }
            return Some(prepared.fetch_all(&mut connection).await.unwrap());
        }).unwrap_or(vec![])
    }
}

impl Iterator for LiteIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() {
            None
        } else {
            Some(self.values.remove(0))
        }
    }
}

impl ValueIterator for LiteIterator {
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
        plan.operate().unwrap()
    }
}