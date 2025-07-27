use crate::mqtt::broker;
use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::station::Command::Ready;
use crate::processing::{plan, Train};
use crate::ui::{ConfigModel, NumberModel, StringModel};
use crate::util::new_broadcast;
use crate::util::new_id;
use crate::util::Tx;
use crossbeam::channel::{unbounded, Sender};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::runtime::Runtime;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct MqttDestination {
    id: usize,
    port: u16,
    url: String,
    sender: Tx<Train>,
}

impl MqttDestination {
    pub fn new(url: String, port: u16) -> Self {
        let sender = new_broadcast("MQTT Destination");
        let id = new_id();
        MqttDestination {
            id,
            port,
            url,
            sender,
        }
    }
}

impl Configurable for MqttDestination {
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

impl Destination for MqttDestination {
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

    fn operate(
        &mut self,
        control: Arc<Sender<Command>>,
    ) -> (Sender<Command>, JoinHandle<Result<(), String>>) {
        let runtime = Runtime::new().unwrap();
        debug!("starting mqtt destination...");

        let id = self.id;
        let receiver = self.sender.subscribe();
        let (tx, rx) = unbounded();
        let url = self.url.clone();
        let port = self.port;

        let (mut broker, mut link_tx, _link_rx) = broker::create_broker(port, url.clone(), id);

        let res = thread::Builder::new()
            .name("MQTT Destination".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    // Start the broker asynchronously
                    tokio::spawn(async move {
                        broker.start().expect("Broker failed to start");
                    });

                    info!("Embedded MQTT broker for sending is running...");

                    link_tx.subscribe("#").unwrap(); // all topics

                    control.send(Ready(id)).unwrap();
                    loop {
                        if plan::check_commands(&rx) {
                            break;
                        }
                        match receiver.try_recv() {
                            Ok(train) => {
                                debug!("Sending {:?}", train);

                                for value in &train.values {
                                    let payload = serde_json::to_string(&value.to_string())
                                        .unwrap_or_else(|err| {
                                            error!("Mqtt payload error {}", err);
                                            String::from("error")
                                        });
                                    match link_tx
                                        .publish("test", payload)
                                        .map_err(|e| e.to_string())
                                    {
                                        Ok(_) => {}
                                        Err(error) => error!("MQTT Error {}", error),
                                    };
                                }
                            }
                            _ => tokio::time::sleep(Duration::from_nanos(100)).await,
                        }
                    }
                    error!("MQTT broker stopped");
                });
                Ok(())
            });

        match res {
            Ok(join) => (tx, join),
            Err(err) => panic!("{}", err),
        }
    }

    fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }

    fn id(&self) -> usize {
        self.id
    }

    fn type_(&self) -> String {
        String::from("MQTT")
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
