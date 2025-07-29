use crate::mqtt::{broker, DEFAULT_URL};
use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command::Ready;
use crate::processing::{plan, Train};
use crate::ui::{ConfigModel, NumberModel, StringModel};
use crate::util::{new_broadcast, HybridThreadPool};
use crate::util::new_id;
use crate::util::Tx;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct MqttDestination {
    id: usize,
    port: u16,
    url: String,
    sender: Tx<Train>,
}

impl MqttDestination {
    pub fn new<S:AsRef<str>>(url: Option<S>, port: u16) -> Self {
        let sender = new_broadcast("MQTT Destination");
        MqttDestination {
            id: new_id(),
            port,
            url: url.map(|r| r.as_ref().to_string()).unwrap_or(DEFAULT_URL.to_string()),
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
    fn parse(options: Map<String, Value>) -> Result<Self, String> {
        let port = if let Some(port) = options.get("port") {
            port.as_i64().unwrap() as u16
        } else {
            return Err(String::from("Port not specified"));
        };

        let url = options.get("url").map(|url| url.as_str()).flatten();

        Ok(Self::new(url, port))
    }

    fn operate(
        &mut self,
        pool: HybridThreadPool,
    ) -> usize {
        debug!("starting mqtt destination...");

        let id = self.id;
        let receiver = self.sender.subscribe();
        let url = self.url.clone();
        let port = self.port;

        let (mut broker, mut link_tx, _link_rx) = broker::create_broker(port, url.clone(), id);

        pool.execute_async(
            "MQTT Destination".to_string(),
            move |meta| {
                Box::pin( async move {
                    // Start the broker asynchronously
                    tokio::spawn(async move {
                        broker.start().expect("Broker failed to start");
                    });

                    info!("Embedded MQTT broker for sending is running...");

                    link_tx.subscribe("#").unwrap(); // all topics

                    meta.output_channel.send(Ready(id));
                    loop {
                        if plan::check_commands(&meta.ins.1) {
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
                })
            }
            ,vec![])
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
