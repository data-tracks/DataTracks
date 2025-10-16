use crate::processing::Train;
use crate::tpc::server::{StreamUser, TcpStream, handle_register};
use crate::tpc::{DEFAULT_URL, Server};
use crate::util::Tx;
use crate::util::deserialize_message;
use crate::util::{HybridThreadPool, Rx};
use core::ConfigModel;
use core::Configurable;
use core::Source;
use core::StringModel;
use crossbeam::channel::{Receiver, Sender};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use threading::command::Command;
use threading::multi::MultiSender;
use tokio::time::sleep;
use tracing::{debug, info, warn};
use track_rails::message_generated::protocol::Payload;
use error::error::TrackError;

#[derive(Clone)]
pub struct TpcSource {
    url: String,
    port: u16,
    control: Option<Arc<Tx<Command>>>,
    outs: Option<MultiSender<Train>>,
}

impl TpcSource {
    pub fn new<S: AsRef<str>>(url: Option<S>, port: u16) -> Self {
        Self {
            url: url
                .map(|r| r.as_ref().to_string())
                .unwrap_or(DEFAULT_URL.to_string()),
            port,
            control: None,
            outs: None,
        }
    }

    fn send(&self, train: Train) -> Result<(), TrackError> {
        self.outs.iter().try_for_each(|out| out.send(train.clone()))
    }

    #[cfg(test)]
    fn operate_test(&mut self) -> (usize, HybridThreadPool) {
        let pool = HybridThreadPool::new();
        let id = self.operate(0, MultiSender::new(vec![]), pool.clone()).unwrap();
        (id, pool)
    }

    fn get_default_configs(&self) -> HashMap<String, ConfigModel> {
        let mut configs = HashMap::new();
        configs.insert(
            String::from("port"),
            ConfigModel::String(StringModel::new("9999")),
        );
        configs
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

impl TryFrom<HashMap<String, ConfigModel>> for TpcSource {
    type Error = String;

    fn try_from(configs: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        let port = if let Some(port) = configs.get("port") {
            port.as_int()?
        } else {
            return Err(String::from("Could not create TpcSource."));
        };
        let url = configs.get("url").map(|s| s.as_str());

        Ok(TpcSource::new(url, port as u16))
    }
}

impl TryFrom<Map<String, Value>> for TpcSource {
    type Error = String;

    fn try_from(options: Map<String, Value>) -> Result<Self, Self::Error> {
        let port = options.get("port").unwrap().as_u64().unwrap_or(9999);
        let url = options.get("url").and_then(|s| s.as_str());

        Ok(TpcSource::new(url, port as u16))
    }
}

impl Source for TpcSource {
    fn operate(&mut self, id: usize, outs: MultiSender<Train>, pool: HybridThreadPool) -> Result<usize, TrackError> {
        debug!("Starting TPC source...");

        let port = self.port;
        let url = self.url.clone();

        self.control = Some(pool.control_sender());
        let control = self.control.clone().unwrap();

        self.outs = Some(outs);

        let clone = self.clone();

        pool.execute_sync("TPC Source", move |meta| {
            let server = Server::new(url.clone(), port);
            server.start(id, clone, control, Arc::new(meta.ins.1))
        })
    }

    fn type_(&self) -> String {
        String::from("TPC")
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

impl StreamUser for TpcSource {
    async fn handle(&mut self, mut stream: TcpStream, rx: Rx<Command>) -> Result<(), TrackError> {
        let mut len_buf = [0u8; 4];

        loop {
            match rx.try_recv() {
                Ok(Command::Stop(_)) => break,
                Err(_) => {}
                _ => {}
            }

            match stream.read_exact(&mut len_buf).await {
                Ok(_) => {
                    let size = u32::from_be_bytes(len_buf) as usize;
                    let mut buffer = vec![0; size];
                    stream.read(&mut buffer).await?;
                    // Deserialize FlatBuffers message
                    if let Ok(msg) = deserialize_message(&buffer) {
                        match msg.data_type() {
                            Payload::RegisterRequest => {
                                info!("tpc registration");
                                stream.write_all(&handle_register()?).await?;
                            }
                            Payload::Train => {
                                let msg = msg.data_as_train().unwrap();

                                //debug!("tpc train: {:?}", msg);
                                match msg.try_into() {
                                    Ok(train) => self.send(train)?,
                                    Err(err) => warn!("error transformation {}", err),
                                }
                            }
                            Payload::Disconnect => {
                                info!("tpc disconnect");
                                break;
                            }
                            Payload::NONE => {
                                info!("tpc none");
                                break;
                            }
                            err => {
                                todo!("other tpc payloads {:?}", err);
                            }
                        }
                    };
                }
                _ => {
                    sleep(Duration::from_millis(10)).await;
                }
            }
        }
        Ok(())
    }

    fn interrupt(&mut self) -> Receiver<Command> {
        todo!()
    }

    fn control(&mut self) -> Sender<Command> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::tpc::TpcSource;
    use rusty_tracks::Client;
    use threading::command::Command::{Ready, Stop};

    #[test]
    fn test_stop() {
        let mut source = TpcSource::new(Some("127.0.0.1"), 9999);

        let (id, pool) = source.operate_test();

        pool.send_control(&id, Stop(0)).unwrap();
        pool.join(&id);
    }

    #[test]
    fn test_stop_connected() {
        let mut source = TpcSource::new(Some("127.0.0.1"), 9991);

        let (id, pool) = source.operate_test();

        match pool.control_receiver().recv() {
            Ok(Ready(_)) => {}
            Err(err) => panic!("{:?}", err),
            _ => {}
        }

        let client = Client::new("127.0.0.1", 9991);
        let mut connection = client.connect().unwrap();

        for _ in 0..100 {
            connection.send("test").unwrap();
        }

        pool.send_control(&id, Stop(0)).unwrap();
        pool.join(&id);
    }
}
