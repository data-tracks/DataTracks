use crate::mqtt::{DEFAULT_URL, broker};
use crate::processing::Train;
use crate::processing::destination::Destination;
use crate::util::HybridThreadPool;
use crate::util::Tx;
use core::ConfigModel;
use core::Configurable;
use core::NumberModel;
use core::StringModel;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::Duration;
use threading::command::Command::Ready;
use tracing::{debug, error, info};
use error::error::TrackError;

impl TryFrom<HashMap<String, ConfigModel>> for MqttDestination {
    type Error = String;

    fn try_from(configs: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        let port = if let Some(port) = configs.get("port") {
            port.as_int()?
        } else {
            return Err(String::from("Could not create MqttSource."));
        };
        let url = configs.get("url").map(|u| u.as_str());

        Ok(MqttDestination::new(url, port as u16))
    }
}

#[derive(Clone)]
pub struct MqttDestination {
    port: u16,
    url: String,
}

impl MqttDestination {
    pub fn new<S: AsRef<str>>(url: Option<S>, port: u16) -> Self {
        MqttDestination {
            port,
            url: url
                .map(|r| r.as_ref().to_string())
                .unwrap_or(DEFAULT_URL.to_string()),
        }
    }

    pub(crate) fn get_default_configs() -> HashMap<String, ConfigModel> {
        let mut map = HashMap::new();
        map.insert(
            String::from("url"),
            ConfigModel::String(StringModel::new(DEFAULT_URL)),
        );
        map.insert(
            String::from("port"),
            ConfigModel::String(StringModel::new("6666")),
        );
        map
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
    fn parse(options: Map<String, Value>) -> Result<Self, TrackError> {
        let port = if let Some(port) = options.get("port") {
            port.as_i64().unwrap() as u16
        } else {
            return Err("Port not specified".into());
        };

        let url = options.get("url").and_then(|url| url.as_str());

        Ok(Self::new(url, port))
    }

    fn operate(&mut self, id: usize, tx: Tx<Train>, pool: HybridThreadPool) -> Result<usize, TrackError> {
        debug!("starting mqtt destination...");

        let url = self.url.clone();
        let port = self.port;

        let (mut broker, mut link_tx, _link_rx) = broker::create_broker(port, url.clone(), id);

        let rx = tx.subscribe();

        pool.execute_async("MQTT Destination", move |meta| {
            Box::pin(async move {
                // Start the broker asynchronously
                tokio::spawn(async move {
                    broker.start().expect("Broker failed to start");
                });

                info!("Embedded MQTT broker for sending is running...");

                link_tx.subscribe("#").unwrap(); // all topics

                meta.output_channel.send(Ready(id)).unwrap();
                loop {
                    if meta.should_stop() {
                        break;
                    }
                    match rx.try_recv() {
                        Ok(train) => {
                            debug!("Sending {:?}", train);

                            for value in &train.into_values() {
                                let payload = serde_json::to_string(&value.to_string())
                                    .unwrap_or_else(|err| {
                                        error!("Mqtt payload error {}", err);
                                        String::from("error")
                                    });
                                match link_tx.publish("test", payload).map_err(|e| e.to_string()) {
                                    Ok(_) => {}
                                    Err(error) => error!("MQTT Error {}", error),
                                };
                            }
                        }
                        _ => tokio::time::sleep(Duration::from_nanos(100)).await,
                    }
                }
                error!("MQTT broker stopped");
                Ok(())
            })
        })
    }

    fn type_(&self) -> String {
        String::from("MQTT")
    }

    fn get_configs(&self) -> HashMap<String, ConfigModel> {
        let mut configs = HashMap::new();
        configs.insert(
            "url".to_string(),
            ConfigModel::String(StringModel::new(&self.url)),
        );
        configs.insert(
            "port".to_string(),
            ConfigModel::Number(NumberModel::new(self.port as i64)),
        );
        configs
    }
}
