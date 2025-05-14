use crate::algebra::{BoxedIterator, ValueIterator};
use crate::analyse::{InputDerivable, OutputDerivationStrategy};
use crate::language::Language;
use crate::processing::option::Configurable;
use crate::processing::transform::{Transform, Transformer};
use crate::processing::{Layout, Train};
use crate::sql::sqlite::connection::SqliteConnector;
use crate::util::{new_id, DynamicQuery};
use crate::value::Value;
use rusqlite::{params_from_iter, ToSql};
use serde_json::Map;
use std::collections::HashMap;
use tokio::runtime::Runtime;

#[derive(Debug, PartialEq, Clone)]
pub struct SqliteTransformer {
    id: usize,
    pub query: DynamicQuery,
    pub connector: SqliteConnector,
    output_derivation_strategy: OutputDerivationStrategy
}

impl SqliteTransformer {
    fn new(query: String, path: String) -> SqliteTransformer {
        let id = new_id();
        let connector = SqliteConnector::new(&path);
        let query = DynamicQuery::build_dynamic_query(query);
        let output_derivation_strategy = OutputDerivationStrategy::query_based(query.get_query(), Language::Sql).unwrap_or_default();
        SqliteTransformer { id, connector, query, output_derivation_strategy }
    }
}

impl Configurable for SqliteTransformer {
    fn name(&self) -> String {
        "SQLite".to_owned()
    }

    fn options(&self) -> Map<String, serde_json::Value> {
        let mut options = Map::new();
        self.connector.add_options(&mut options);
        options.insert(String::from("query"), serde_json::Value::String(self.query.get_query()));
        options
    }
}

impl InputDerivable for SqliteTransformer {
    fn derive_input_layout(&self) -> Option<Layout> {
        Some(self.query.derive_input_layout())
    }

}

impl Transformer for SqliteTransformer {
    fn parse(options: Map<String, serde_json::Value>) -> Result<Self, String> {
        let query = options.get("query").unwrap().as_str().ok_or("Could not find query option.")?;
        let path = options.get("path").unwrap().as_str().ok_or("Could not find path option.".to_string())?;
        Ok(SqliteTransformer::new(query.to_owned(), path.to_owned()))
    }

    fn optimize(&self, _transforms: HashMap<String, Transform>) -> Box<dyn ValueIterator<Item=Value> + Send> {
        let iter = SqliteIterator::new(self.query.clone(), self.connector.clone());

        Box::new(iter)
    }

    fn get_output_derivation_strategy(&self) -> &OutputDerivationStrategy {
        &self.output_derivation_strategy
    }
}

pub struct SqliteIterator {
    query: DynamicQuery,
    connector: SqliteConnector,
    values: Vec<Value>
}

impl SqliteIterator {
    pub fn new(query: DynamicQuery, connector: SqliteConnector) -> SqliteIterator {
        SqliteIterator { query, connector, values: Vec::new() }
    }

    fn query_values(&self, value: Value) -> Vec<Value> {
        let runtime = Runtime::new().unwrap();
        let query = self.query.clone();
        runtime.block_on(async {
            let connection = self.connector.connect().await.unwrap();
            let (query, value_function) = query.prepare_query("$", None);
            let mut prepared = connection.prepare_cached(&query).unwrap();
            let count = prepared.column_count();
            let mut iter = prepared.query(params_from_iter(value_function(&value).iter().map(|v| v.to_sql().unwrap()))).unwrap();
            let mut values = vec![];
            while let Ok(Some(row)) = iter.next() {
                values.push((row, count).try_into().unwrap());
            }
            values
        })
    }
}

impl Iterator for SqliteIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() {
            None
        } else {
            Some(self.values.remove(0))
        }
    }
}

impl ValueIterator for SqliteIterator {
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
        Box::new(SqliteIterator::new(self.query.clone(), self.connector.clone()))
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}


#[cfg(test)]
mod tests {
    use crate::processing::Plan;

    const PLAN: &str = "\
            0--1{sql|SELECT \"id\", \"name\" FROM $0, $lite($0.id)}\n\
            \n\
            Transform\n\
            $lite:SQLite{\"path\":\"memory:\",\"query\":\"SELECT \\\"id\\\" FROM \\\"company\\\" WHERE \\\"name\\\" = $\"}";

    #[test]
    fn test_simple_parse() {
        let plan = Plan::parse(PLAN).unwrap();
        assert_eq!(plan.dump(false).replace("\n", ""), PLAN.replace("\n", ""));
    }

    #[test]
    fn test_simple_operate() {
        let mut plan = Plan::parse(PLAN).unwrap();
        plan.operate().unwrap()
    }

}