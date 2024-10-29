use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::station::Command::{Ready, Stop};
use crate::processing::Train;
use crate::ui::{ConfigModel, StringModel};
use crate::util::{Tx, GLOBAL_ID};
use crate::value::{Dict, Value};
use crossbeam::channel::{unbounded, Sender};
use log::error;
use rumqttc::{Client, Event, Incoming, MqttOptions};
use rumqttd::protocol::Publish;
use rumqttd::Meter::Router;
use rumqttd::{BridgeConfig, Broker, Config, ConnectionSettings, Notification, RouterConfig, ServerSettings};
use serde_json::Map;
use std::collections::{BTreeMap, HashMap};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::thread::{sleep, spawn};
use std::time::Duration;
use std::{str, thread};
use tokio::runtime::Runtime;
use tracing::{debug, warn};
use tracing_subscriber::fmt::format;

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
        let runtime = Runtime::new().unwrap();
        debug!("starting mqtt source...");

        let (tx, rx) = unbounded();
        let outs = self.outs.clone();
        let port = self.port;
        let url = self.url.clone();
        let id = self.id;

        spawn(move || {
            let mut config = Config::default();

            config.router = RouterConfig {
                max_connections: 100,
                max_outgoing_packet_count: 100,
                max_segment_size: 100000000,
                max_segment_count: 100000,
                ..Default::default()
            };

            config.v4 = Some(
                HashMap::from([
                    (id.to_string(), ServerSettings {
                        name: id.to_string(),
                        listen: SocketAddr::V4(SocketAddrV4::new(url.parse().unwrap(), port)),
                        tls: None,
                        next_connection_delay_ms: 0,
                        connections: ConnectionSettings {
                            connection_timeout_ms: 10000,
                            max_payload_size: 10000000,
                            max_inflight_count: 1000,
                            auth: None,
                            external_auth: None,
                            dynamic_filters: false,
                        },
                    })
                ])
            );
            // Create the broker with the configuration
            let mut broker = Broker::new(config);
            let (mut link_tx, mut link_rx) = broker.link("link").unwrap();

            runtime.block_on(async move {

                // Start the broker asynchronously
                tokio::spawn(async move {
                    broker.start().expect("Broker failed to start");
                });

                warn!("Embedded MQTT broker is running...");
                control.send(Ready(id)).unwrap();
                warn!("started");

                link_tx.subscribe("#").unwrap(); // all topics

                loop {
                    match rx.try_recv() {
                        Ok(command) => match command {
                            Stop(_) => break,
                            _ => {}
                        },
                        _ => {}
                    }

                    let notification = match link_rx.recv().unwrap() {
                        Some(v) => v,
                        None => continue,
                    };

                    error!("message: {:?}", notification);
                    match notification {
                        Notification::Forward(f) => {
                            let mut dict = BTreeMap::new();
                            dict.insert("$data".to_string(), Value::text(str::from_utf8(&f.publish.payload).unwrap()));
                            dict.insert("$topic".to_string(), Value::text(str::from_utf8(&f.publish.topic).unwrap()));
                            send_message(Value::dict(dict).as_dict().unwrap(), &outs)
                        }
                        msg => {
                            warn!("Received unexpected message: {:?}", msg);
                        }
                    }
                }

                warn!("MQTT broker has been stopped.");
            });
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

    fn from(configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String> {
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

impl TryFrom<Notification> for Dict {
    type Error = String;

    fn try_from(value: Notification) -> Result<Self, Self::Error> {
        match value {
            Notification::Forward(f) => {
                f.publish.try_into()
            }
            _ => Err(format!("Unexpected notification {:?}", value))?
        }
    }
}

impl TryFrom<Publish> for Dict {
    type Error = String;

    fn try_from(publish: Publish) -> Result<Self, Self::Error> {
        let mut dict = BTreeMap::new();
        let value = str::from_utf8(&publish.payload).map_err(|e| e.to_string())?.into();
        let topic = str::from_utf8(&publish.topic).map_err(|e| e.to_string())?.into();
        dict.insert("$data".to_string(), value);
        dict.insert("$topic".to_string(), topic);
        Ok(Value::dict(dict).into())
    }
}

impl TryFrom<Event> for Dict {
    type Error = String;

    fn try_from(value: Event) -> Result<Self, Self::Error> {
        match value {
            Event::Incoming(i) => {
                match i {
                    Incoming::Publish(p) => {
                        let mut map = BTreeMap::new();
                        map.insert("$data".to_string(), Value::text(str::from_utf8(&p.payload).map_err(|e| e.to_string())?.into()));
                        map.insert("$topic".to_string(), Value::text(&p.topic));
                        Ok(Value::dict(map).as_dict().unwrap())
                    }
                    _ => Err(format!("Unexpected Incoming publish {:?}", i))?
                }
            }
            Event::Outgoing(_) => {
                Err(String::from("Unexpected Outgoing publish"))
            }
        }
    }
}



