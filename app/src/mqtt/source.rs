use crate::mqtt::{DEFAULT_URL, broker};
use crate::processing::Train;
use crate::util::HybridThreadPool;
use core::ConfigModel;
use core::Configurable;
use core::Source;
use core::StringModel;
use rumqttd::Notification;
use serde_json::Map;
use std::collections::{BTreeMap, HashMap};
use std::str;
use std::time::Duration;
use threading::command::Command::Ready;
use threading::multi::MultiSender;
use tracing::{debug, info, warn};
use error::error::TrackError;
use value::{Dict, Value};

// mosquitto_sub -h 127.0.0.1 -p 8888 -t "test/topic2" -i "id"
// mosquitto_pub -h 127.0.0.1 -p 6666 -t "test/topic2" -m "Hello fromtods2" -i "testclient"
#[derive(Clone)]
pub struct MqttSource {
    url: String,
    port: u16,
}

impl Configurable for MqttSource {
    fn name(&self) -> String {
        String::from("Mqtt")
    }

    fn options(&self) -> Map<String, serde_json::Value> {
        let mut options = serde_json::map::Map::new();
        options.insert(
            "url".to_string(),
            serde_json::Value::String(self.url.clone()),
        );
        options.insert(
            "port".to_string(),
            serde_json::Value::Number(self.port.into()),
        );
        options
    }
}

impl MqttSource {
    pub fn new<S: AsRef<str>>(url: Option<S>, port: u16) -> Self {
        MqttSource {
            port,
            url: url
                .map(|r| r.as_ref().to_string())
                .unwrap_or(DEFAULT_URL.to_string()),
        }
    }

    pub fn get_default_configs() -> HashMap<String, ConfigModel> {
        let mut configs = HashMap::new();
        configs.insert(
            String::from("port"),
            ConfigModel::String(StringModel::new("7777")),
        );
        configs
    }
}

impl TryFrom<HashMap<String, ConfigModel>> for MqttSource {
    type Error = String;

    fn try_from(configs: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        let port = if let Some(port) = configs.get("port") {
            port.as_int()?
        } else {
            return Err(String::from("Could not create MqttSource."));
        };
        let url = configs.get("url").map(|u| u.as_str());

        Ok(MqttSource::new(url, port as u16))
    }
}

impl TryFrom<Map<String, serde_json::Value>> for MqttSource {
    type Error = String;

    fn try_from(options: Map<String, serde_json::Value>) -> Result<Self, Self::Error> {
        let port = options.get("port").unwrap().as_u64().unwrap_or(9999);
        let url = options.get("url").and_then(|url| url.as_str());
        Ok(MqttSource::new(url, port as u16))
    }
}

impl Source for MqttSource {
    fn operate(&mut self, id: usize, outs: MultiSender<Train>, pool: HybridThreadPool) -> Result<usize, TrackError> {
        debug!("starting mqtt source...");

        let port = self.port;
        let url = self.url.clone();

        let control = pool.control_sender();

        pool.execute_async("MQTT Source", move |meta| {
            let (mut broker, mut link_tx, mut link_rx) = broker::create_broker(port, url, id);
            Box::pin(async move {
                // Start the broker asynchronously
                tokio::spawn(async move {
                    broker.start().expect("Broker failed to start");
                });

                info!("Embedded MQTT broker for receiving is running...");
                control.send(Ready(id))?;

                link_tx.subscribe("#").map_err(|err| err.to_string())?; // all topics

                loop {
                    if meta.should_stop() {
                        break;
                    }
                    if let Some(notification) = link_rx.recv().unwrap() {
                        match notification {
                            Notification::Forward(f) => {
                                let mut dict = BTreeMap::new();
                                dict.insert(
                                    "$".to_string(),
                                    Value::text(str::from_utf8(&f.publish.payload).unwrap()),
                                );
                                dict.insert(
                                    "$topic".to_string(),
                                    Value::text(str::from_utf8(&f.publish.topic).unwrap()),
                                );
                                send_message(Value::dict(dict).as_dict().unwrap(), &outs)?
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
            String::from("port"),
            ConfigModel::String(StringModel::new(&self.port.to_string())),
        );
        configs
    }
}

pub fn send_message(dict: Dict, outs: &MultiSender<Train>) -> Result<(), TrackError> {
    let train = Train::new_values(vec![Value::Dict(dict)], 0, 0);
    outs.send(train)
}
