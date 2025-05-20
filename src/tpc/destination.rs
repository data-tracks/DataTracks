use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use crossbeam::channel::{unbounded, Receiver, Sender};
use serde_json::{Map, Value};
use tokio::time::sleep;
use tracing::{debug, error};
use crate::processing::{Train};

use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::util::{new_channel, new_id, Rx, Tx};
use crate::tpc::Server;
use crate::tpc::server::{StreamUser, TcpStream};
use crate::ui::{ConfigModel, NumberModel, StringModel};

#[derive(Clone)]
pub struct TpcDestination {
    id: usize,
    port: u16,
    url: String,
    receiver: Rx<Train>,
    sender: Tx<Train>,
    tx: Sender<Command>,
    rx: Receiver<Command>,
    control: Option<Arc<Sender<Command>>>,
}

impl TpcDestination {
    pub fn new(url: String, port: u16) -> Self {
        let (tx, rx) = new_channel();
        let id = new_id();
        
        let (t,r) = unbounded();
      
        TpcDestination { id, port, url, receiver: rx, sender: tx, tx:t, rx:r, control: None }
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

        control.send(Command::Ready(self.id)).unwrap();
        loop {
            match self.rx.try_recv() {
                Ok(msg) => {
                    panic!("msg: {:?}", msg);
                }
                Err(_) => {}
            }
            match self.receiver.try_recv() {
                Ok(msg) => {
                    match serde_json::to_string(&msg) {
                        Ok(json) => stream.write_all(json.as_bytes()).await.unwrap(),
                        Err(_) => {}
                    }
                }
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
        let tx_clone = tx.clone();
        let rx = self.rx.clone();
        
        let clone = self.clone();
        let res = thread::Builder::new().name("TPC Destination".to_string()).spawn(move || {
            server.start(clone, Arc::new(tx_clone), Arc::new(rx)).unwrap();
        });

        match res {
            Ok(_) => {}
            Err(err) => error!("{}", err)
        }
        
        tx
        
    }

    fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }

    fn get_id(&self) -> usize {
        self.id
    }

    fn serialize(&self) -> DestinationModel {
        let mut configs = HashMap::new();
        configs.insert("url".to_string(), ConfigModel::String(StringModel::new(&self.url)));
        configs.insert("port".to_string(), ConfigModel::Number(NumberModel::new(self.port as i64)));
        DestinationModel { type_name: self.name(), id: self.id.to_string(), configs }
    }

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized,
    {
        None
    }
}