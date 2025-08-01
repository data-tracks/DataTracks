use crate::util::Container;
use crate::Manager;
use crossbeam::channel::{Receiver, Sender};
use mongodb::bson::Document;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::options::ClientOptions;
use mongodb::Client;
use std::thread::{spawn, JoinHandle};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::time::sleep;
use value::Value;

pub struct MongoDbCdc {
    client: Client,
    runtime: Runtime,
}


impl MongoDbCdc {
    pub fn new<S: AsRef<str>>(name: S) -> Result<Self, String> {
        let manager = Manager::new()?;
        manager.init_and_reset_container(name, Container::mongo_db())?;

        Self::connect(
            "127.0.0.1",
            27017,
            Builder::new_multi_thread().worker_threads(4)
                .enable_all()
                .build()
                .map_err(|s| s.to_string())?,
        )
    }

    fn connect<S: AsRef<str>>(url: S, port: u16, runtime: Runtime) -> Result<Self, String> {
        let connection_uri = format!("mongodb://{}:{}/?directConnection=true&serverSelectionTimeoutMS=2000&replicaSet=repl", url.as_ref(), port);

        let client = runtime.block_on(async {
            sleep(Duration::from_secs(5)).await;
            let options = ClientOptions::parse(connection_uri)
                .await
                .map_err(|e| e.to_string())?;

            Client::with_options(options).map_err(|err| err.to_string())
        })?;

        Ok(MongoDbCdc { client, runtime })
    }

    pub async fn get_client(&self) -> Result<Client, String> {
        let connection_uri = format!("mongodb://{}:{}/?directConnection=true&serverSelectionTimeoutMS=2000&replicaSet=repl", "localhost", 27017);
        let options = ClientOptions::parse(connection_uri)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Client::with_options(options).map_err(|err| err.to_string())?)
    }


    pub fn listen(&self) -> Result<CdcHandle, String> {
        let (tx_in, rx_in) = crossbeam::channel::unbounded();
        let (tx_out, rx_out) = crossbeam::channel::unbounded();


        let tx_thread = tx_out.clone();
        let handle = spawn(move || {
            let rt = Builder::new_multi_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let connection_uri = format!("mongodb://{}:{}/?directConnection=true&serverSelectionTimeoutMS=2000&replicaSet=repl", "localhost", 27017);
                let options = ClientOptions::parse(connection_uri)
                    .await
                    .map_err(|e| e.to_string())?;
                let client = Client::with_options(options).map_err(|err| err.to_string())?;

                let mut change_stream = client.watch().await.map_err(|err| err.to_string())?;

                loop {
                    match rx_in.try_recv() {
                        Ok(_) => break,
                        Err(_) => {}
                    }
                    match change_stream.next_if_any().await.map_err(|err| err.to_string())? {
                        Some(event) => {
                            let value = event.into();
                            tx_thread.send(value).map_err(|err| err.to_string())?;
                        }
                        None => {
                            sleep(Duration::from_millis(100)).await;
                        }
                    }
                }

                Ok(())
            })
        });

        Ok(CdcHandle {
            handle: Some(handle),
            in_sender: tx_in,
            out: (tx_out, rx_out),
        })
    }
}

pub struct CdcHandle {
    handle: Option<JoinHandle<Result<(), String>>>,
    in_sender: Sender<bool>,
    out: (Sender<CdcEvent>, Receiver<CdcEvent>),
}

impl Drop for CdcHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.in_sender
                .send(true)
                .map_err(|_| "Cdc disconnected")
                .unwrap();

            handle
                .join()
                .map_err(|_| "Cdc disconnected")
                .unwrap()
                .unwrap();
        }
    }
}

#[derive(Debug)]
pub struct CdcEvent {
    pub operation_type: CdcEventType,
    pub value: Value
}

impl From<ChangeStreamEvent<Document>> for CdcEvent {
    fn from(change: ChangeStreamEvent<Document>) -> Self {
        CdcEvent{
            operation_type: change.operation_type.into(),
            value: change.full_document.unwrap_or_default().into(),
        }
    }
}



#[derive(Debug)]
pub enum CdcEventType {
    Insert,
    Update,
    Delete,
    Other,
}

impl From<OperationType> for CdcEventType {
    fn from(operation_type: OperationType) -> Self {
        match operation_type {
            OperationType::Insert => CdcEventType::Insert,
            OperationType::Update => CdcEventType::Update,
            OperationType::Delete => CdcEventType::Delete,
            _ => CdcEventType::Other,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::mongo::replication::MongoDbCdc;
    use mongodb::{ Collection};
    use serde::Serialize;
    use std::time::Duration;
    use tracing::info;
    use tracing_test::traced_test;

    #[derive(Serialize)]
    struct Book{
        title: String,
    }

    #[test]
    #[traced_test]
    fn init_test() {
        let client = MongoDbCdc::new("test").unwrap();

        client.runtime.block_on(async {
            let col: Collection<Book> = client.client.database("db").collection("test");
            col.insert_one(Book{title: "test".to_string()}).await
        }).unwrap();

    }

    #[test]
    #[traced_test]
    fn listen_test() {
        let cdc = MongoDbCdc::new("test").unwrap();

        // watch for events
        let watch = cdc.listen().unwrap();

        // change stuff
        let rt = &cdc.runtime;
        rt.block_on(async {
            let client = cdc.get_client().await.unwrap();

            let col: Collection<Book> = client.database("db").collection("test");
            col.insert_one(Book{title: "test".to_string()}).await
        }).unwrap();
        // wait to trickle dow
        std::thread::sleep(Duration::from_secs(10));

        let res = watch.out.1.try_recv();
        println!("{:?}", res);
        assert!(res.is_ok());
        info!("in: {:?}", res.unwrap());

        drop(watch);
    }
}
