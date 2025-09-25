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

#[cfg(test)]
mod test {
    use crate::mongo::replication::{Identifier, MongoDbCdc};
    use crate::util::ChangeDataCapture;
    use crate::{Container, Manager};
    use mongodb::bson::{Document, doc};
    use mongodb::{Client, Collection};
    use serde::Serialize;
    use tokio::runtime::Runtime;

    #[derive(Serialize)]
    struct Book {
        title: String,
    }

    pub fn start_docker_new<S: AsRef<str>>(
        name: S,
        port: u16,
        entity: Identifier,
    ) -> Result<MongoDbCdc, String> {
        let manager = Manager::new()?;
        manager.init_and_reset_container(name.as_ref(), Container::mongo_db("127.0.0.1", port))?;

        MongoDbCdc::new("127.0.0.1", port, entity)
    }

    #[test]
    fn init_test() {
        let client = start_docker_new("mongo_init", 27016, Identifier::default()).unwrap();

        Runtime::new()
            .unwrap()
            .block_on(async {
                let col: Collection<Book> = client
                    .get_client()
                    .await
                    .unwrap()
                    .database("db")
                    .collection("test");
                col.insert_one(Book {
                    title: "test".to_string(),
                })
                .await
            })
            .unwrap();

        Manager::new()
            .unwrap()
            .remove_container("mongo_init")
            .unwrap()
    }

    #[test]
    fn insert_test() {
        let func = |client: Client| async move {
            let col: Collection<Book> = client.database("db").collection("test");
            col.insert_one(Book {
                title: "test".to_string(),
            })
            .await
            .unwrap();
        };

        run_cdc(func, "test_insert", 27018, 1);
    }

    #[test]
    //#[traced_test]
    fn update_test() {
        let func = |client: Client| {
            async move {
                let col: Collection<Book> = client.database("db").collection("test");
                col.insert_one(Book {
                    title: "test".to_string(),
                })
                .await
                .unwrap(); // only sends update if something was updated
                col.update_one(
                    Document::new(),
                    doc! {
                        "$set": { "title": "test1" },
                    },
                )
                .await
                .unwrap();
            }
        };

        run_cdc(func, "test_update", 27019, 2);
    }

    #[test]
    //#[traced_test]
    fn delete_test() {
        let func = |client: Client| {
            async move {
                let col: Collection<Book> = client.database("db").collection("test");
                col.insert_one(Book {
                    title: "test".to_string(),
                })
                .await
                .unwrap(); // only sends delete if something was deleted
                col.delete_one(Document::new()).await.unwrap();
            }
        };

        run_cdc(func, "test_delete", 27020, 2);
    }

    #[test]
    //#[traced_test]
    fn transaction_test() {
        let func = |client: Client| async move {
            let mut session = client.start_session().await.unwrap();
            session.start_transaction().await.unwrap();

            let col: Collection<Book> = session.client().database("db").collection("test");
            col.insert_one(Book {
                title: "test1".to_string(),
            })
            .session(&mut session)
            .await
            .unwrap();
            col.insert_one(Book {
                title: "test2".to_string(),
            })
            .session(&mut session)
            .await
            .unwrap();

            session.commit_transaction().await.unwrap();
        };

        run_cdc(func, "test_transaction", 27021, 2);
    }

    #[test]
    //#[traced_test]
    fn transaction2_test() {
        let func = |client: Client| async move {
            let mut session = client.start_session().await.unwrap();
            session.start_transaction().await.unwrap();

            let col: Collection<Book> = session.client().database("db").collection("test");
            col.insert_one(Book {
                title: "test1".to_string(),
            })
            .session(&mut session)
            .await
            .unwrap();
            col.update_one(
                Document::new(),
                doc! {
                    "$set": { "title": "test3" },
                },
            )
            .session(&mut session)
            .await
            .unwrap();

            session.commit_transaction().await.unwrap();
        };

        run_cdc(func, "test_transaction2", 27022, 2);
    }

    fn run_cdc<F, Fut>(func: F, name: &str, port: u16, results: u16)
    where
        F: FnOnce(Client) -> Fut,
        Fut: Future<Output = ()>,
    {
        let name = format!("mongo_{}", name);

        let mut cdc = start_docker_new(name.clone(), port, Identifier::default()).unwrap();

        // watch for events
        let (pool, tx) = cdc.listen_test().unwrap();

        // change stuff
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let client = cdc.get_client().await.unwrap();
            func(client).await;
        });

        let receiver = tx.subscribe();

        for _ in 0..results {
            let res = receiver.recv().unwrap();
            println!("got: {:?}", res);
        }

        drop(pool);

        Manager::new().unwrap().remove_container(name).unwrap();
    }
}
