use crate::mqtt::broker;
use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::station::Command::{Ready, Stop};
use crate::processing::Train;
use crate::util::{new_channel, Rx, Tx, GLOBAL_ID};
use crossbeam::channel::{unbounded, Sender};
use log::warn;
use rumqttc::{Client, MqttOptions, QoS};
use serde_json::{Map, Value};
use std::sync::Arc;
use std::thread::{sleep, spawn};
use std::time::Duration;
use tokio::runtime::Runtime;
use tracing::debug;

pub struct MqttDestination {
    id: i64,
    port: u16,
    url: String,
    receiver: Rx<Train>,
    sender: Tx<Train>,
}

impl MqttDestination {
    pub fn new(url: String, port: u16) -> Self {
        let (tx, _num, rx) = new_channel();
        let id = GLOBAL_ID.new_id();
        MqttDestination { id, port, url, receiver: rx, sender: tx }
    }
}

impl Configurable for MqttDestination {
    fn get_name(&self) -> String {
        String::from("Mqtt")
    }

    fn get_options(&self) -> Map<String, Value> {
        Map::new()
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

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        let runtime = Runtime::new().unwrap();
        debug!("starting mqtt destination...");

        let id = self.id;
        let receiver = self.receiver.clone();
        let (tx, rx) = unbounded();
        let url = self.url.clone();
        let port = self.port;

        let (mut broker, mut link_tx, _link_rx) = broker::create_broker(port, url.clone(), id);

        spawn(move || {
            runtime.block_on(async move {

                // Start the broker asynchronously
                tokio::spawn(async move {
                    broker.start().expect("Broker failed to start");
                });

                warn!("Embedded MQTT broker for sending is running...");
                control.send(Ready(id)).unwrap();

                link_tx.subscribe("#").unwrap(); // all topics

                control.send(Ready(id)).unwrap();
                loop {
                    if let Ok(command) = rx.try_recv() {
                        match command {
                            Stop(_) => break,
                            _ => {}
                        }
                    }
                    match receiver.try_recv() {
                        Ok(train) => {
                            warn!("Sending {:?}", train);
                            if let Some(packet) = train.values {
                                let payload = serde_json::to_string(&packet).unwrap();
                                link_tx.publish("test/topic2", payload).map_err(|e| e.to_string()).unwrap();
                            }
                        }
                        _ => tokio::time::sleep(Duration::from_nanos(100)).await
                    }
                }
            });

        });
        tx
    }

    fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }

    fn get_id(&self) -> i64 {
        self.id
    }

    fn serialize(&self) -> DestinationModel {
        todo!()
    }

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized,
    {
        todo!()
    }
}
