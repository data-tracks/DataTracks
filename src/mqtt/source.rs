use crate::mqtt::broker;
use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::station::Command::Ready;
use crate::processing::{plan, Train};
use crate::ui::{ConfigModel, StringModel};
use crate::util::{new_id, Tx};
use crate::value::{Dict, Value};
use crossbeam::channel::{unbounded, Sender};
use rumqttc::{Event, Incoming};
use rumqttd::protocol::Publish;
use rumqttd::Notification;
use serde_json::Map;
use std::collections::{BTreeMap, HashMap};
use std::str;
use std::sync::Arc;
use std::thread::spawn;
use std::time::Duration;
use tokio::runtime::Runtime;
use tracing::{debug, warn};

pub struct MqttSource {
    id: usize,
    url: String,
    port: u16,
    outs: Vec<Tx<Train>>,
}


impl Configurable for MqttSource {
    fn name(&self) -> String {
        String::from("Mqtt")
    }

    fn options(&self) -> Map<String, serde_json::Value> {
        let mut options = serde_json::map::Map::new();
        options.insert("url".to_string(), serde_json::Value::String(self.url.clone()));
        options.insert("port".to_string(), serde_json::Value::Number(self.port.into()));
        options
    }
}

impl MqttSource {
    pub fn new(url: String, port: u16) -> Self {
        MqttSource { port, url, id: new_id(), outs: Vec::new() }
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
            let (mut broker, mut link_tx, mut link_rx) = broker::create_broker(port, url, id);

            runtime.block_on(async move {

                // Start the broker asynchronously
                tokio::spawn(async move {
                    broker.start().expect("Broker failed to start");
                });

                warn!("Embedded MQTT broker is running...");
                control.send(Ready(id)).unwrap();

                link_tx.subscribe("#").unwrap(); // all topics

                loop {
                    if plan::check_commands(&rx) { break; }
                    if let Some(notification) = link_rx.recv().unwrap() {
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
                    } else {
                        tokio::time::sleep(Duration::from_nanos(100)).await;
                    }
                }
                warn!("MQTT broker has been stopped.");
            });
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

pub fn send_message(dict: Dict, outs: &Vec<Tx<Train>>) {
    let train = Train::new(usize::MAX, vec![Value::Dict(dict)]);
    for tx in outs {
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
                        map.insert("$data".to_string(), Value::text(str::from_utf8(&p.payload).map_err(|e| e.to_string())?));
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



