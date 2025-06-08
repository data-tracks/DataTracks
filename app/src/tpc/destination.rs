use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::tpc::server::{ack, StreamUser, TcpStream};
use crate::tpc::Server;
use crate::ui::{ConfigModel, NumberModel, StringModel};
use crate::util::new_broadcast;
use crate::util::new_id;
use crate::util::Tx;
use crossbeam::channel::{unbounded, Receiver, Sender};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error};

#[derive(Clone)]
pub struct TpcDestination {
    id: usize,
    port: u16,
    url: String,
    sender: Tx<Train>,
    tx: Sender<Command>,
    rx: Receiver<Command>,
    control: Option<Arc<Sender<Command>>>,
}

impl TpcDestination {
    pub fn new(url: String, port: u16) -> Self {
        let tx = new_broadcast("TPC Destination");
        let id = new_id();

        let (t, r) = unbounded();

        TpcDestination {
            id,
            port,
            url,
            sender: tx,
            tx: t,
            rx: r,
            control: None,
        }
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
    async fn handle(&mut self, mut stream: TcpStream) {
        let control = self.control.clone().unwrap();

        let receiver = self.sender.subscribe();

        match ack(&mut stream).await {
            Ok(_) => {}
            Err(err) => {
                error!("TPC Destination Register{:?}", err);
                return;
            }
        }

        control.send(Command::Ready(self.id)).unwrap();
        let mut retry = 3;
        loop {
            if let Ok(msg) = self.rx.try_recv() {
                panic!("msg: {:?}", msg);
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
    fn parse(_options: Map<String, Value>) -> Result<Self, String> {
        let port = if let Some(port) = _options.get("port") {
            port.as_i64().unwrap() as u16
        } else {
            return Err(String::from("Port not specified"));
        };

        let url = if let Some(url) = _options.get("url") {
            url.as_str().unwrap().to_string()
        } else {
            return Err(String::from("MqttDestination URL is required"));
        };

        Ok(Self::new(url, port))
    }

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        debug!("starting tpc destination...");

        let url = self.url.clone();
        let port = self.port;

        let server = Server::new(url.clone(), port);
        self.control = Some(control);

        let tx = self.tx.clone();
        let rx = self.rx.clone();

        let clone = self.clone();

        let res = thread::Builder::new()
            .name("TPC Destination".to_string())
            .spawn(move || {
                server.start(clone, Arc::new(tx), Arc::new(rx)).unwrap();
            });

        match res {
            Ok(_) => {}
            Err(err) => error!("{}", err),
        }

        self.tx.clone()
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
