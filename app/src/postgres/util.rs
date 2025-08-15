use crate::postgres::connection::PostgresConnection;
use crate::util::{DynamicQuery, ValueExtractor};
use core::util::iterator::BoxedValueIterator;
use core::util::iterator::ValueIterator;
use core::util::reservoir::ValueReservoir;
use postgres::types::ToSql;
use postgres::{Client, Statement};
use std::collections::HashMap;
use std::rc::Rc;

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
    pub fn new(query: DynamicQuery, connector: PostgresConnection) -> Result<Self, String> {
        let (q, value_functions) = query.prepare_query_transform("$", None, 1)?;
        let con = connector.clone();

        let mut client = con.connect()?;

        let statement = client.prepare(&q).map_err(|err| err.to_string())?;

        let client = connector.clone().connect()?;

        Ok(PostgresIterator {
            client,
            query,
            connector,
            statement,
            value_functions,
            values: vec![],
            storage: Default::default(),
        })
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

    fn clone_boxed(&self) -> BoxedValueIterator {
        Box::new(PostgresIterator::new(
            self.query.clone(),
            self.connector.clone(),
        ).unwrap())
    }

    fn enrich(
        &mut self,
        _transforms: Rc<HashMap<String, BoxedValueIterator>>,
    ) -> Option<BoxedValueIterator> {
        None
    }
}
