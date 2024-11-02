use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::station::Command::{Ready, Stop};
use crate::processing::{plan, Train};
use crate::sql::sqlite::connection::SqliteConnector;
use crate::ui::ConfigModel;
use crate::util::{Tx, GLOBAL_ID};
use crate::value::value;
use crossbeam::channel::{unbounded, Sender};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct LiteSource {
    id: i64,
    connector: SqliteConnector,
    outs: Vec<Tx<Train>>,
    query: String,
}

impl LiteSource {
    pub fn new(path: String, query: String) -> LiteSource {
        let id = GLOBAL_ID.new_id();
        let connection = SqliteConnector::new(path.as_str());
        LiteSource { id, connector: connection, outs: Vec::new(), query }
    }
}

impl Configurable for LiteSource {
    fn get_name(&self) -> String {
        "SQLite".to_string()
    }

    fn get_options(&self) -> Map<String, Value> {
        let mut options = Map::new();
        options.insert(String::from("query"), Value::String(self.query.clone()));
        self.connector.add_options(&mut options);
        options
    }
}

impl Source for LiteSource {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        let query = options.get("query").unwrap().as_str().unwrap();
        let path = options.get("path").unwrap().as_str().unwrap();
        Ok(LiteSource::new(path.to_owned(), query.to_owned()))
    }

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        let (tx, rx) = unbounded();
        let id = self.id.clone();
        let query = self.query.to_owned();
        let runtime = Runtime::new().unwrap();
        let connection = self.connector.clone();
        let sender = self.outs.clone();

        runtime.block_on(async {
            let mut conn = connection.connect().unwrap();
            control.send(Ready(id)).unwrap();
            loop {
                if plan::check_commands(&rx) { break; }

                let query = sqlx::query_as(query.as_str());
                let values: Vec<value::Value> = query.fetch_all(&mut conn).await.unwrap();
                let train = Train::new(-1, values);

                for sender in &sender {
                    sender.send(train.clone()).unwrap();
                }

            }
            control.send(Stop(id)).unwrap();
        });
        tx
    }

    fn get_outs(&mut self) -> &mut Vec<Tx<Train>> {
        &mut self.outs
    }


    fn get_id(&self) -> i64 {
        self.id
    }

    fn serialize(&self) -> SourceModel {
        let mut configs = HashMap::new();
        self.connector.serialize(&mut configs);
        configs.insert("query".to_string(), ConfigModel::text(self.query.as_str()));
        SourceModel { type_name: String::from("SQLite"), id: self.id.to_string(), configs }
    }

    fn from(configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
    where
        Self: Sized,
    {
        let query = if let Some(query) = configs.get("query") {
            query.as_str()
        } else {
            return Err(String::from("Could not create SQLiteSource."))
        };
        let path = if let Some(path) = configs.get("path") {
            path.as_str()
        } else {
            return Err(String::from("Could not create MqttSource."))
        };

        Ok(Box::new(LiteSource::new(path, query)))
    }

    fn serialize_default() -> Result<SourceModel, ()>
    where
        Self: Sized,
    {
        Ok(SourceModel { type_name: String::from("SQLite"), id: String::from("SQLite"), configs: HashMap::new() })
    }
}

#[cfg(test)]
mod tests {
    use crate::processing::Plan;
    use sqlx::{Connection, Executor, SqliteConnection};
    use tokio::runtime::Runtime;

    fn create_sqlite_source(path: &str) {
        let err: Result<(), String> = Runtime::new().unwrap().block_on(async {
            // Define the database URL for a persistent file
            let database_url = format!("sqlite://{}", path);

            // Create the database connection pool
            let mut connection = SqliteConnection::connect(&database_url).await.map_err(|e| e.to_string())?;

            // Initialize a table in the database
            connection.execute("CREATE TABLE IF NOT EXISTS user (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
                .await.map_err(|e| e.to_string())?;

            // Insert some sample data into the table
            sqlx::query("INSERT INTO users (name) VALUES (?1), (?2)")
                .bind("Alice".to_string())
                .bind("Bob".to_string())
                .fetch_all(&mut connection)
                .await.map_err(|e| e.to_string())?;
            Ok(())
        });
        if err.is_err() {
            panic!("{}", err.err().unwrap())
        }
    }

    #[test]
    fn test_simple_source() {
        create_sqlite_source("test.db");
        let plan = format!(
            "\
            0--1\n\
            In\n\
            Sqlite{{\"path\":\"test.db\",\"query\":\"SELECT * FROM test\"}}:0\n\
            Out\n\
            Dummy{{\"id\": 35, \"result_size\":2}}:1\
            "
        );
        let mut plan = Plan::parse(&plan).unwrap();

        let dummy = plan.get_result(35).clone();
        plan.operate().unwrap();

        for _ in 0..4 {
            plan.control_receiver.1.recv().unwrap();
        }
        let values = dummy.lock().unwrap();
        println!("{:?}", values);
    }
}



