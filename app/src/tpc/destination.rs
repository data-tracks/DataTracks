use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::tpc::server::{ack, StreamUser, TcpStream};
use crate::tpc::{Server, DEFAULT_URL};
use crate::ui::{ConfigModel, NumberModel, StringModel};
use crate::util::{new_broadcast, HybridThreadPool};
use crate::util::new_id;
use crate::util::Rx;
use crate::util::Tx;
use crossbeam::channel::{Receiver, Sender};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error};

#[derive(Clone)]
pub struct TpcDestination {
    id: usize,
    port: u16,
    url: String,
    sender: Tx<Train>,
    control: Option<Arc<Tx<Command>>>,

}

impl TpcDestination {
    pub fn new<S: AsRef<str>>(url: Option<S>, port: u16) -> Self {
        let tx = new_broadcast("TPC Destination");

        TpcDestination {
            id: new_id(),
            port,
            url: url.map(|r| r.as_ref().to_string()).unwrap_or(DEFAULT_URL.to_string()),
            sender: tx,
            control: None,
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Configurable for TpcDestination {
    fn name(&self) -> String {
        String::from("Mqtt")
    }

    fn options(&self) -> Map<String, Value> {
        let mut options = Map::new();
        options.insert(String::from("url"), Value::String(self.url.clone()));
        options.insert(String::from("port"), Value::Number(self.port.into()));
        options
    }
}

impl StreamUser for TpcDestination {
    async fn handle(&mut self, mut stream: TcpStream, rx: Rx<Command>) {
        let control = self.control.clone().unwrap();

        let receiver = self.sender.subscribe();

        match ack(&mut stream).await {
            Ok(_) => {}
            Err(err) => {
                error!("TPC Destination Register{:?}", err);
                return;
            }
        }

        control.send(Command::Ready(self.id));
        let mut retry = 3;
        loop {
            match rx.try_recv() {
                Ok(Command::Stop(_)) => break,
                Err(_) => {}
                _ => {}
            }

            match receiver.try_recv() {
                Ok(msg) => match stream.write_all(&<Train as Into<Vec<u8>>>::into(msg)).await {
                    Ok(_) => {
                        retry = 3;
                    }
                    Err(err) => {
                        if retry < 1 {
                            error!("Error TPC Destination disconnected {:?}", err);
                            return;
                        }
                        retry -= 1;
                    }
                },
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    fn interrupt(&mut self) -> Receiver<Command> {
        todo!()
    }

    fn control(&mut self) -> Sender<Command> {
        todo!()
    }
}

impl Destination for TpcDestination {
    fn parse(options: Map<String, Value>) -> Result<Self, String> {
        let port = if let Some(port) = options.get("port") {
            port.as_i64().unwrap() as u16
        } else {
            return Err(String::from("Port not specified"));
        };

        let url =  options.get("url").map(|url| url.as_str()).flatten();

        Ok(Self::new(url, port))
    }

    fn operate(
        &mut self,
        pool: HybridThreadPool,
    ) -> usize {
        debug!("starting tpc destination...");

        let url = self.url.clone();
        let port = self.port;

        let server = Server::new(url.clone(), port);

        self.control = Some(pool.control_sender());

        let clone = self.clone();

        let id = self.id;

        let id = pool.execute_sync("TPC Destination".to_string(), move |meta| {
            match server.start(id, clone, meta.output_channel, Arc::new(meta.ins.1)) {
                Ok(_) => {}
                Err(err) => error!("Error on TPC Destination thread{:?}", err),
            }
        }, vec![]);


        id

    }

    fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }

    fn id(&self) -> usize {
        self.id
    }

    fn type_(&self) -> String {
        String::from("TPC")
    }

    fn serialize(&self) -> DestinationModel {
        let mut configs = HashMap::new();
        configs.insert(
            "url".to_string(),
            ConfigModel::String(StringModel::new(&self.url)),
        );
        configs.insert(
            "port".to_string(),
            ConfigModel::Number(NumberModel::new(self.port as i64)),
        );
        DestinationModel {
            type_name: self.name(),
            id: self.id.to_string(),
            configs,
        }
    }

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized,
    {
        None
    }
}
