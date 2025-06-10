use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::tpc::server::{handle_register, StreamUser, TcpStream};
use crate::tpc::Server;
use crate::ui::{ConfigModel, StringModel};
use crate::util::Tx;
use crate::util::{deserialize_message, new_id};
use crossbeam::channel::{unbounded, Receiver, Sender};
use schemas::message_generated::protocol::Payload;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

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
            id: new_id(),
            url,
            port,
            outs: Vec::new(),
        }
    }

    fn send(&self, train: Train) {
        self.outs.iter().for_each(|out| out.send(train.clone()))
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

impl Source for TpcSource {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        let port = options.get("port").unwrap().as_u64().unwrap_or(9999);
        let url = options
            .get("url")
            .unwrap()
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        Ok(TpcSource::new(url, port as u16))
    }

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        debug!("starting tpc source...");

        let (tx, rx) = unbounded();
        let port = self.port;
        let url = self.url.clone();
        let rx = Arc::new(rx);

        let clone = self.clone();

        let res = thread::Builder::new()
            .name("TPC Source".to_string())
            .spawn(move || {
                let server = Server::new(url.clone(), port);
                match server.start(clone, control, rx) {
                    Ok(_) => {}
                    Err(_) => {}
                }
            });

        match res {
            Ok(_) => {}
            Err(err) => error!("{:?}", err),
        }

        tx
    }

    fn outs(&mut self) -> &mut Vec<Tx<Train>> {
        &mut self.outs
    }

    fn id(&self) -> usize {
        self.id
    }

    fn type_(&self) -> String {
        String::from("TPC")
    }

    fn serialize(&self) -> SourceModel {
        SourceModel {
            type_name: String::from("Tpc"),
            id: self.id.to_string(),
            configs: HashMap::new(),
        }
    }

    fn from(configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
    where
        Self: Sized,
    {
        let port = if let Some(port) = configs.get("port") {
            port.as_int()?
        } else {
            return Err(String::from("Could not create TpcSource."));
        };
        let url = if let Some(url) = configs.get("url") {
            url.as_str()
        } else {
            return Err(String::from("Could not create TpcSource."));
        };

        Ok(Box::new(TpcSource::new(url.to_owned(), port as u16)))
    }

    fn serialize_default() -> Result<SourceModel, ()>
    where
        Self: Sized,
    {
        let mut configs = HashMap::new();
        configs.insert(
            String::from("port"),
            ConfigModel::String(StringModel::new("9999")),
        );
        Ok(SourceModel {
            type_name: String::from("Tpc"),
            id: String::from("Tpc"),
            configs,
        })
    }
}

impl StreamUser for TpcSource {
    async fn handle(&mut self, mut stream: TcpStream) {
        let mut len_buf = [0u8; 4];

        loop {
            match stream.read_exact(&mut len_buf).await {
                Ok(_) => {
                    let size = u32::from_be_bytes(len_buf) as usize;
                    let mut buffer = vec![0; size];
                    stream.read(&mut buffer).await.unwrap();
                    // Deserialize FlatBuffers message
                    match deserialize_message(&buffer) {
                        Ok(msg) => match msg.data_type() {
                            Payload::RegisterRequest => {
                                info!("tpc registration");
                                stream.write_all(&handle_register().unwrap()).await.unwrap();
                            }
                            Payload::Train => {
                                let msg = msg.data_as_train().unwrap();

                                match msg.try_into() {
                                    Ok(train) => self.send(train),
                                    Err(err) => warn!("error transformation {}", err),
                                }
                            }
                            _ => {
                                todo!("other tpc payloads")
                            }
                        },
                        Err(_) => (),
                    };
                }
                _ => {
                    sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    fn interrupt(&mut self) -> Receiver<Command> {
        todo!()
    }

    fn control(&mut self) -> Sender<Command> {
        todo!()
    }
}
