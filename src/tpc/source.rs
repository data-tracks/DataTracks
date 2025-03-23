use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::{plan, Train};
use crate::ui::{ConfigModel, StringModel};
use crate::util::{new_id, Tx};
use crossbeam::channel::{unbounded, Receiver, Sender};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::io::Error;
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::Duration;
use rumqttd::Notification;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tracing::{debug, warn};
use crate::management::{Storage, API};
use crate::processing::station::Command::Ready;
use crate::tpc::Server;
use crate::tpc::server::StreamUser;

#[derive(Clone)]
pub struct TpcSource {
    id: usize,
    url: String,
    port: u16,
    outs: Vec<Tx<Train>>,
}

impl TpcSource {

    pub fn new(url: String, port: u16) -> Self {
        Self {
            id: new_id(), url, port, outs: Vec::new(),
        }
    }
}

impl Configurable for TpcSource {
    fn name(&self) -> String {
        "TpcSource".to_string()
    }

    fn options(&self) -> Map<String, Value> {
        let mut options = serde_json::map::Map::new();
        options.insert("url".to_string(), Value::String(self.url.clone()));
        options.insert("port".to_string(), Value::Number(self.port.into()));
        options
    }
}

impl Source for TpcSource{
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized
    {
        let port = options.get("port").unwrap().as_u64().unwrap_or(9999);
        let url = options.get("url").unwrap().as_str().unwrap().parse().unwrap();
        Ok(TpcSource::new(url, port as u16))
    }

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        debug!("starting tpc source...");

        let (tx, rx) = unbounded();
        let outs = self.outs.clone();
        let port = self.port;
        let url = self.url.clone();
        let id = self.id;

        let clone = self.clone();

        spawn(move || {
            let server = Server::new(url.clone(), port);
            match server.start(clone) {
                Ok(_) => {}
                Err(_) => {}
            }
        });
        tx
    }

    fn outs(&mut self) -> &mut Vec<Tx<Train>> {
        &mut self.outs
    }

    fn id(&self) -> usize {
        self.id
    }

    fn serialize(&self) -> SourceModel {
        SourceModel { type_name: String::from("Tpc"), id: self.id.to_string(), configs: HashMap::new() }
    }

    fn from(configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
    where
        Self: Sized
    {
        let port = if let Some(port) = configs.get("port") {
            port.as_int()?
        } else {
            return Err(String::from("Could not create TpcSource."))
        };
        let url = if let Some(url) = configs.get("url") {
            url.as_str()
        } else {
            return Err(String::from("Could not create TpcSource."))
        };

        Ok(Box::new(TpcSource::new(url.to_owned(), port as u16)))
    }

    fn serialize_default() -> Result<SourceModel, ()>
    where
        Self: Sized
    {
        let mut configs = HashMap::new();
        configs.insert(String::from("port"), ConfigModel::String(StringModel::new("9999")));
        Ok(SourceModel { type_name: String::from("Tpc"), id: String::from("Tpc"), configs })
    }
}

impl StreamUser for TpcSource {
    async fn handle(&mut self, stream: TcpStream, storage: Arc<Mutex<Storage>>, api: Arc<Mutex<API>>) {
        todo!()
    }

    fn interrupt(&mut self) -> Receiver<Command> {
        todo!()
    }

    fn control(&mut self) -> Sender<Command> {
        todo!()
    }
}