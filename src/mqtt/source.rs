use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::station::Command::Ready;
use crate::processing::{OutputType, Train};
use crate::ui::{ConfigModel, StringModel};
use crate::util::{Tx, GLOBAL_ID};
use crate::value::{Dict, Value};
use crossbeam::channel::{unbounded, Sender};
use mqtt_packet_3_5::{MqttPacket, PacketDecoder, PublishPacket};
use rumqttc::{Client, Event, MqttOptions, Outgoing, Packet};
use serde_json::Map;
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use tokio::runtime::Runtime;
use tracing::{debug, warn};

pub struct MqttSource {
    id: i64,
    url: String,
    port: u16,
    outs: HashMap<i64, Tx<Train>>,
}


impl MqttSource {
    pub fn new(url: String, port: u16) -> Self {
        MqttSource { port, url, id: GLOBAL_ID.new_id(), outs: HashMap::new() }
    }
}

impl Configurable for MqttSource {
    fn get_name(&self) -> String {
        String::from("Mqtt")
    }

    fn get_options(&self) -> Map<String, serde_json::Value> {
        let mut options = serde_json::map::Map::new();
        options.insert("url".to_string(), serde_json::Value::String(self.url.clone()));
        options.insert("port".to_string(), serde_json::Value::Number(self.port.into()));
        options
    }
}

impl Source for MqttSource {
    fn parse(options: Map<String, serde_json::Value>) -> Result<Self, String> {
        let port = options.get("port").unwrap().as_u64().unwrap_or(9999);
        let url = options.get("url").unwrap().as_str().unwrap().parse().unwrap();
        Ok(MqttSource::new(url, port as u16))
    }

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        debug!("starting mqtt source...");

        let (tx, rx) = unbounded();
        let outs = self.outs.clone();
        let port = self.port;
        let url = self.url.clone();
        let id = self.id;

        thread::spawn(move || {
            let options = MqttOptions::new("id", url, port);
            let (_client, mut connection) = Client::new(options, 10);
            control.send(Ready(id)).unwrap();
            loop {
                if let Ok(command) = rx.try_recv() {
                    match command {
                        Command::Stop(_) => {
                            break
                        }
                        _ => {}
                    }
                }
                if let Ok(message) = connection.try_recv() {
                    if let Ok(message) = message {
                        send_message(message.into(), &outs)
                    }
                } else {
                    sleep(Duration::from_nanos(10))
                }
            }


        });
        tx
    }

    fn add_out(&mut self, id: i64, out: Tx<Train>) {
        self.outs.insert(id, out);
    }

    fn get_id(&self) -> i64 {
        self.id
    }

    fn serialize(&self) -> SourceModel {
        SourceModel { type_name: String::from("Mqtt"), id: self.id.to_string(), configs: HashMap::new() }
    }

    fn from( configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String> {
        let port = if let Some(port) = configs.get("port") {
            port.as_int()?
        } else {
            return Err(String::from("Could not create MqttSource."))
        };
        let url = if let Some(url) = configs.get("url") {
            url.as_str()
        } else {
            return Err(String::from("Could not create MqttSource."))
        };

        Ok(Box::new(MqttSource::new(url.to_owned(), port as u16)))
    }

    fn serialize_default() -> Result<SourceModel, ()> {
        let mut configs = HashMap::new();
        configs.insert(String::from("port"), ConfigModel::String(StringModel::new("7777")));
        Ok(SourceModel { type_name: String::from("Mqtt"), id: String::from("Mqtt"), configs })
    }
}

pub fn send_message(dict: Dict, outs: &HashMap<i64, Tx<Train>>) {
    let train = Train::new(-1, vec![Value::Dict(dict)]);
    for tx in outs.values() {
        tx.send(train.clone()).unwrap();
    }
}

impl From<Event> for Dict {
    fn from(value: Event) -> Self {
        match value {
            Event::Incoming(i) => {
                i.into()
            }
            Event::Outgoing(o) => {
                o.into()
            }
        }
    }
}

impl From<Packet> for Dict {
    fn from(value: Outgoing) -> Self {
        value.
    }
}

