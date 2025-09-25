use crate::mongo::util::MongoDynamicQuery;
use crate::processing::destination::Destination;
use cdc::{MongoDbCdc, MongoIdentifier};
use core::models::configuration::ConfigModel;
use core::Configurable;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::Duration;
use threading::channel::Tx;
use threading::command::Command::Ready;
use threading::pool::HybridThreadPool;
use tracing::debug;
use error::error::TrackError;
use value::train::Train;

#[derive(Clone)]
pub struct MongoDbDestination {
    database: String,
    url: String,
    port: u16,
    pub query: MongoDynamicQuery,
}

impl MongoDbDestination {
    pub fn new<S0: AsRef<str>, S1: AsRef<str>, S2: AsRef<str>>(
        url: S0,
        port: u16,
        query: S1,
        database: S2,
    ) -> Result<Self, TrackError> {
        let query = MongoDynamicQuery::new(query.as_ref())?;
        Ok(MongoDbDestination {
            database: database.as_ref().to_string(),
            url: url.as_ref().to_string(),
            port,
            query,
        })
    }
}

impl Configurable for MongoDbDestination {
    fn name(&self) -> String {
        "MongoDB".to_string()
    }

    fn options(&self) -> Map<String, Value> {
        let mut options = Map::new();
        options.insert(String::from("url"), Value::String(self.url.to_string()));
        options.insert(String::from("port"), Value::Number(self.port.into()));
        options.insert(String::from("query"), Value::String(self.query.get_query()));
        options
    }
}

impl TryFrom<HashMap<String, ConfigModel>> for MongoDbDestination {
    type Error = TrackError;

    fn try_from(configs: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        let port = if let Some(port) = configs.get("port") {
            port.as_int()?
        } else {
            return Err(TrackError::from("Could not create MqttSource."));
        };
        let url = if let Some(url) = configs.get("url") {
            url.as_str()
        } else {
            return Err(TrackError::from("No url provided"));
        };

        let query = if let Some(query) = configs.get("query") {
            query.as_str()
        } else {
            return Err(TrackError::from("No query provided"));
        };

        let db = if let Some(db) = configs.get("database") {
            db.as_str()
        } else {
            return Err(TrackError::from("No database provided"));
        };


        MongoDbDestination::new(url, port as u16, query, db)
    }
}

impl Destination for MongoDbDestination {
    fn parse(options: Map<String, Value>) -> Result<Self, TrackError>
    where
        Self: Sized,
    {
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
            .ok_or(error("database"))?
            .to_string();

        MongoDbDestination::new(url, port, query, db)
    }

    fn operate(&mut self, id: usize, tx: Tx<Train>, pool: HybridThreadPool) -> Result<usize, TrackError> {
        let cdc = MongoDbCdc::new(
            self.url.clone(),
            self.port,
            MongoIdentifier::new(None, None),
        )?;

        let rx = tx.subscribe();
        let db = self.database.clone();
        let query = self.query.clone();

        pool.execute_async(format!("MongoDestination_{}", id), move |meta| {
            Box::pin(async move {
                let client = cdc
                    .get_client()
                    .await
                    .map_err(|_| "Could not connect to mongo".to_string())?;

                let action = query.as_fn()?;

                let collection = action.prepare(client.database(&db))?;

                meta.output_channel.send(Ready(id)).unwrap();
                loop {
                    if meta.should_stop() {
                        break;
                    }

                    match rx.try_recv() {
                        Ok(train) => {
                            debug!("Handling {:?}", train);
                            for value in train.into_values() {
                                action.execute(value, &collection).await?;
                            }

                        }
                        _ => tokio::time::sleep(Duration::from_nanos(100)).await,
                    }
                }

                Ok(())
            })
        })
    }

    fn type_(&self) -> String {
        String::from("Postgres")
    }

    fn get_configs(&self) -> HashMap<String, ConfigModel> {
        let mut map = HashMap::new();
        map.insert(
            "query".to_string(),
            ConfigModel::text(self.query.get_query()),
        );
        map.insert("url".to_string(), ConfigModel::text(self.url.to_string()));
        map.insert("port".to_string(), ConfigModel::number(self.port as i64));
        map
    }
}

fn error(msg: &str) -> String {
    format!("Parse error {}", msg)
}

#[cfg(test)]
pub mod test {
    use std::thread::sleep;
    use super::*;
    use cdc::{Container, Manager, PostgresIdentifier};
    use futures_util::StreamExt;
    use mongodb::bson::doc;
    use mongodb::Collection;
    use std::time::Instant;
    use threading::channel::new_channel;
    use threading::command::Command::Stop;
    use tokio::runtime::Runtime;
    use tracing_test::traced_test;
    use r#macro::limited;
    use crate::processing::Plan;
    use crate::tests::init_postgres_table;
    use crate::tests::plan_test::tests;

    #[test]
    fn test_simple() {
        let container = "mongo_destination".to_string();
        let mgr = Manager::new().unwrap();
        mgr.init_and_reset_container(&container, Container::mongo_db("localhost", 3838) ).unwrap();

        let mut destination = MongoDbDestination::new("localhost", 3838, "db.col.insert($0)", "testing").unwrap();

        let (tx, _) = new_channel("test_mongo_destination",false);
        let pool = HybridThreadPool::new();

        let id = destination.operate(0, tx.clone(), pool.clone()).unwrap();

        tx.send(Train::new_values(vec![value::Value::dict_from_kv("test", value::Value::text("test"))], 0, 0)).unwrap();
        tx.send(Train::new_values(vec![value::Value::dict_from_kv("test", value::Value::text("test2"))], 0, 1)).unwrap();

        let rt = Runtime::new().unwrap();

        rt.block_on(async {
            let cdc = MongoDbCdc::new("localhost", 3838, MongoIdentifier::new(None, None)).unwrap();

            let client = cdc.get_client().await.unwrap();

            let col: Collection<Value> = client.database("testing").collection("col");

            let mut values = vec![];

            let instant = Instant::now();

            while values.len() < 2 && instant.elapsed() < Duration::from_secs(12) {
                let mut dics = col.find(doc! {}).await.map_err(|err| "Error in MongoDB operation!").unwrap();

                while let Some(value) = dics.next().await {
                    values.push(value.unwrap());
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            assert_eq!(values.len(), 2);;
            println!("Duration {:?}", instant.elapsed());
        });

        pool.send_control(&id, Stop(0)).unwrap();
        drop(pool);

        mgr.stop_container(&container).unwrap();
        mgr.remove_container(&container).unwrap();

    }

    #[test]
    #[limited(s = 50)]
    #[traced_test]
    fn test_plan_mongodb() {
        let container = "mongo_destination_plan".to_string();
        let mgr = Manager::new().unwrap();
        mgr.init_and_reset_container(&container, Container::mongo_db("localhost", 3131)).unwrap();

        let values: Vec<value::Value> = vec![
            value::Value::dict_from_kv("name", value::Value::text("Peter")),
            value::Value::dict_from_kv("name", value::Value::text("Hans")),
        ];
        let source_id = 1;

        let mut plan = Plan::parse(&format!(
            "\
            0--1\n\
            \n\
            In\n\
            Dummy{{\"id\": {source_id}, \"delay\":{delay},\"values\":{values}}}:0\n\
            Out\n\
            MongoDb{{\"url\":\"localhost\", \"port\":3131, \"query\":\"db.col.insert($1)\", \"database\": \"testing\"}}:1", // did already pass 1
            delay = 1,
            values = tests::dump(std::slice::from_ref(&values))
        ))
            .unwrap();

        match plan.operate() {
            Ok(_) => {}
            Err(err) => panic!("err {}", err),
        }

        plan.send_control(&source_id, Ready(0)).unwrap();

        let rt = Runtime::new().unwrap();

        rt.block_on(async {
            let cdc = MongoDbCdc::new("localhost", 3131, MongoIdentifier::new(None, None)).unwrap();

            let client = cdc.get_client().await.unwrap();

            let col: Collection<Value> = client.database("testing").collection("col");

            let mut values = vec![];

            let instant = Instant::now();

            while values.len() < 2 && instant.elapsed() < Duration::from_secs(12) {
                let mut dics = col.find(doc! {}).await.map_err(|err| format!("Error in MongoDB operation! {}", err)).unwrap();

                while let Some(value) = dics.next().await {
                    values.push(value.unwrap());
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            assert_eq!(values.len(), 2);
            println!("Duration {:?}", instant.elapsed());
        });

        drop(plan);

        let mgt = Manager::new().unwrap();
        mgt.stop_container(&container).unwrap();
        mgt.remove_container(&container).unwrap();
    }


}