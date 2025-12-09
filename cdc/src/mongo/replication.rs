use crate::util::ChangeDataCapture;
use mongodb::Client;
use mongodb::options::ClientOptions;
use std::time::Duration;
use threading::command::Command::Ready;
use threading::multi::MultiSender;
use threading::pool::HybridThreadPool;
use tokio::time::sleep;
use tracing::debug;
use error::error::TrackError;
use value::train::Train;

#[derive(Default, Clone)]
pub struct Identifier {
    pub database: Option<String>,
    pub collection: Option<String>,
}

impl Identifier {
    pub fn new(database: Option<String>, collection: Option<String>) -> Identifier {
        Identifier {
            database,
            collection,
        }
    }
}

/// Mongo sends committed and preserved events
pub struct MongoDbCdc {
    target: Identifier,
    url: String,
    port: u16,
}

impl Default for MongoDbCdc {
    fn default() -> Self {
        MongoDbCdc::new("127.0.0.1", 27017, Identifier::default()).unwrap()
    }
}

impl MongoDbCdc {
    pub fn new<S1: AsRef<str>>(url: S1, port: u16, target: Identifier) -> Result<Self, String> {
        Ok(MongoDbCdc {
            target,
            url: url.as_ref().to_string(),
            port,
        })
    }

    pub async fn get_client(&self) -> Result<Client, String> {
        let connection_uri = self.get_connection_string();
        let options = ClientOptions::parse(connection_uri)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Client::with_options(options).map_err(|err| err.to_string())?)
    }

    fn get_connection_string(&self) -> String {
        format!(
            "mongodb://{}:{}/?directConnection=true&serverSelectionTimeoutMS=2000&replicaSet=repl",
            self.url, self.port
        )
    }
}

impl ChangeDataCapture for MongoDbCdc {
    fn listen(
        &mut self,
        id: usize,
        outs: MultiSender<Train>,
        pool: HybridThreadPool,
    ) -> Result<usize, TrackError> {
        let connection_uri = self.get_connection_string();

        let target = self.target.clone();

        pool.execute_async(format!("Mongo_CDC_{}", id), move |meta| {
            Box::pin(async move {
                let mut counter = 0;
                let options = ClientOptions::parse(connection_uri).await.unwrap();
                let client = Client::with_options(options).unwrap();

                // get element to observe
                let mut change_stream = if let Some(database) = &target.database {
                    let database = client.database(database);
                    if let Some(collection) = &target.collection {
                        let col = client.database(collection);
                        col.watch().await
                    } else {
                        database.watch().await
                    }
                } else {
                    client.watch().await
                }
                .map_err(|e| e.to_string())?;

                meta.output_channel.send(Ready(id))?;
                loop {
                    if meta.should_stop() {
                        break;
                    }

                    match change_stream.next_if_any().await.unwrap() {
                        Some(event) => {
                            let value = (id, counter, event.into()).into();
                            debug!("Handling {:?}", value);
                            match outs.send(value) {
                                Ok(_) => counter += 1,
                                Err(err) => Err(err)?,
                            };
                        }
                        None => {
                            sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
                Ok(())
            })
        })
    }
}


