use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Sources::Tpc;
use crate::processing::source::{Source, Sources};
use crate::processing::station::Command;
use crate::processing::Train;
use crate::tpc::server::{handle_register, StreamUser, TcpStream};
use crate::tpc::{Server, DEFAULT_URL};
use crate::ui::{ConfigModel, StringModel};
use crate::util::Tx;
use crate::util::{deserialize_message, new_id};
use crate::util::{HybridThreadPool, Rx};
use crossbeam::channel::{Receiver, Sender};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use track_rails::message_generated::protocol::Payload;

#[derive(Clone)]
pub struct TpcSource {
    id: usize,
    url: String,
    port: u16,
    outs: Vec<Tx<Train>>,
    control: Option<Arc<Tx<Command>>>,
}

impl TpcSource {
    pub fn new<S: AsRef<str>>(url: Option<S>, port: u16) -> Self {
        Self {
            id: new_id(),
            url: url.map(|r| r.as_ref().to_string()).unwrap_or(DEFAULT_URL.to_string()),
            port,
            outs: Vec::new(),
            control: None,
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
        let url = options.get("url").map(|s| s.as_str()).flatten();

        Ok(TpcSource::new(url, port as u16))
    }


    fn operate(
        &mut self,
        pool: HybridThreadPool,
    ) -> usize {
        debug!("Starting TPC source...");

        let port = self.port;
        let url = self.url.clone();

        self.control = Some(pool.control_sender());
        let control = self.control.clone().unwrap();

        let clone = self.clone();

        pool.execute_sync("TPC Source", move |meta| {
            let server = Server::new(url.clone(), port);
            match server.start(clone.id, clone, control, Arc::new(meta.ins.1)) {
                Ok(_) => {}
                Err(err) => error!("Error on TPC source: {}", err),
            }
        }, vec![])

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

    fn from(configs: HashMap<String, ConfigModel>) -> Result<Sources, String>
    where
        Self: Sized,
    {
        let port = if let Some(port) = configs.get("port") {
            port.as_int()?
        } else {
            return Err(String::from("Could not create TpcSource."));
        };
        let url = configs.get("url").map(|s| s.as_str());

        Ok(Tpc(TpcSource::new(url, port as u16)))
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
    async fn handle(&mut self, mut stream: TcpStream, rx: Rx<Command>) {
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
                    stream.read(&mut buffer).await.unwrap();
                    // Deserialize FlatBuffers message
                    if let Ok(msg) = deserialize_message(&buffer) {
                        match msg.data_type() {
                            Payload::RegisterRequest => {
                                info!("tpc registration");
                                stream.write_all(&handle_register().unwrap()).await.unwrap();
                            }
                            Payload::Train => {
                                let msg = msg.data_as_train().unwrap();

                                //debug!("tpc train: {:?}", msg);
                                match msg.try_into() {
                                    Ok(train) => self.send(train),
                                    Err(err) => warn!("error transformation {}", err),
                                }
                            }
                            Payload::Disconnect => {
                                info!("tpc disconnect");
                                return;
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
    use crate::processing::source::Source;
    use crate::processing::station::Command::{Ready, Stop};
    use crate::tpc::TpcSource;
    use rusty_tracks::Client;

    #[test]
    fn test_stop() {
        let mut source = TpcSource::new(Some("127.0.0.1"), 9999);

        let (id, pool) = source.operate_test();

        pool.send_control(&id, Stop(0));
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

        pool.send_control(&id, Stop(0));
        pool.join(&id);
    }
}
