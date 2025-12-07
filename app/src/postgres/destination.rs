use crate::postgres::connection::PostgresConnection;
use crate::processing::destination::Destination;
use crate::util::{DynamicQuery, HybridThreadPool, Tx};
use core::ConfigModel;
use core::Configurable;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;
use tracing::debug;
use error::error::TrackError;
use threading::command::Command::Ready;
use value::train::Train;

#[derive(Clone)]
pub struct PostgresDestination {
    pub(crate) connector: PostgresConnection,
    pub(crate) query: DynamicQuery,
}

impl PostgresDestination {
    pub fn new<S1: AsRef<str>, S2: AsRef<str>, S3: AsRef<str>, S4: AsRef<str>>(
        url: S1,
        port: u16,
        db: S2,
        query: S3,
        user: S4,
    ) -> Self {
        let query = DynamicQuery::build_dynamic_query(query.as_ref());
        let connector = PostgresConnection::new(url.as_ref(), port, db, user);

        PostgresDestination { connector, query }
    }
}

impl Configurable for PostgresDestination {
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

impl TryFrom<HashMap<String, ConfigModel>> for PostgresDestination {
    type Error = String;

    fn try_from(configs: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        let port = if let Some(port) = configs.get("port") {
            port.as_int()?
        } else {
            return Err(String::from("Could not create MqttSource."));
        };
        let url = if let Some(url) = configs.get("url") {
            url.as_str()
        } else {
            return Err(String::from("No url provided"));
        };

        let query = if let Some(query) = configs.get("query") {
            query.as_str()
        } else {
            return Err(String::from("No query provided"));
        };

        let db = if let Some(db) = configs.get("database") {
            db.as_str()
        } else {
            return Err(String::from("No database provided"));
        };

        let user = if let Some(user) = configs.get("user") {
            user.as_str()
        } else {
            return Err(String::from("No database provided"));
        };

        Ok(PostgresDestination::new(url, port as u16, db, query, user))
    }
}

impl Destination for PostgresDestination {
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
            .ok_or(error("database name"))?
            .to_string();
        let user = options
            .get("user")
            .and_then(Value::as_str)
            .ok_or(error("user name"))?
            .to_string();
        Ok(PostgresDestination::new(url, port, db, query, user))
    }

    fn operate(
        &mut self,
        id: usize,
        tx: Tx<Train>,
        pool: HybridThreadPool,
    ) -> Result<usize, TrackError> {
        let query = self.query.clone();

        let (query, _) = query.prepare_query_transform("$", None, 1)?;

        let mut client = self.connector.connect()?;

        let statement = client.prepare(&query).map_err(|err| err.to_string())?;

        let rx = tx.subscribe();

        pool.execute_sync("Postgres Destination", move |meta| {
            meta.output_channel.send(Ready(id))?;
            loop {
                if meta.should_stop() {
                    break;
                }
                match rx.try_recv() {
                    Ok(train) => {
                        let values = &train.into_values();
                        if values.is_empty() {
                            continue;
                        }
                        for value in values {
                            //let values: &[&(dyn ToSql + Sync)] = &values;

                            let added = client
                                .execute(&statement, &[value])
                                .map_err(|err| err.to_string())?;
                            debug!("added: {}", added);
                        }
                    }
                    _ => sleep(Duration::from_nanos(100)),
                }
            }
            Ok(())
        })
    }

    fn type_(&self) -> String {
        String::from("Postgres")
    }

    fn get_configs(&self) -> HashMap<String, ConfigModel> {
        let mut map = self.connector.serialize();
        map.insert(
            "query".to_string(),
            ConfigModel::text(self.query.get_query()),
        );
        map
    }
}

fn error(msg: &str) -> String {
    format!("Parse error {}", msg)
}
