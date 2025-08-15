use crate::processing::Train;
use crate::processing::destination::Destination;
use crate::tpc::server::{StreamUser, TcpStream, ack};
use crate::tpc::{DEFAULT_URL, Server};
use crate::util::HybridThreadPool;
use crate::util::Rx;
use crate::util::Tx;
use core::ConfigModel;
use core::Configurable;
use core::NumberModel;
use core::StringModel;
use crossbeam::channel::{Receiver, Sender};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use threading::command::Command;
use tokio::time::sleep;
use tracing::{debug, error};

#[derive(Clone)]
pub struct TpcDestination {
    port: u16,
    url: String,
    run_parameter: Option<(Arc<Tx<Command>>, Tx<Train>, usize)>,
}

impl TpcDestination {
    pub fn new<S: AsRef<str>>(url: Option<S>, port: u16) -> Self {
        TpcDestination {
            port,
            url: url
                .map(|r| r.as_ref().to_string())
                .unwrap_or(DEFAULT_URL.to_string()),
            run_parameter: None,
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl TryFrom<HashMap<String, ConfigModel>> for TpcDestination {
    type Error = String;

    fn try_from(configs: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        let port = if let Some(port) = configs.get("port") {
            port.as_int()?
        } else {
            return Err(String::from("Could not create TpcDestination."));
        };
        let url = configs.get("url").map(|u| u.as_str());

        Ok(TpcDestination::new(url, port as u16))
    }
}

impl Configurable for TpcDestination {
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

impl StreamUser for TpcDestination {
    async fn handle(&mut self, mut stream: TcpStream, rx: Rx<Command>) -> Result<(), String> {
        let (control, sender, id) = self.run_parameter.clone().unwrap();
        let receiver = sender.subscribe();

        match ack(&mut stream).await {
            Ok(_) => {}
            Err(err) => {
                error!("TPC Destination Register{:?}", err);
                return Ok(());
            }
        }

        control.send(Command::Ready(id))?;
        let mut retry = 3;
        loop {
            match rx.try_recv() {
                Ok(Command::Stop(_)) => break,
                Err(_) => {}
                _ => {}
            }

            match receiver.try_recv() {
                Ok(msg) => match stream.write_all(&<Train as Into<Vec<u8>>>::into(msg)).await {
                    Ok(_) => {
                        retry = 3;
                    }
                    Err(err) => {
                        if retry < 1 {
                            error!("Error TPC Destination disconnected {:?}", err);
                            return Err(err.to_string());
                        }
                        retry -= 1;
                    }
                },
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
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

impl Destination for TpcDestination {
    fn parse(options: Map<String, Value>) -> Result<Self, String> {
        let port = if let Some(port) = options.get("port") {
            port.as_i64().unwrap() as u16
        } else {
            return Err(String::from("Port not specified"));
        };

        let url = options.get("url").and_then(|url| url.as_str());

        Ok(Self::new(url, port))
    }

    fn operate(&mut self, id: usize, tx: Tx<Train>, pool: HybridThreadPool) -> Result<usize, String> {
        debug!("starting tpc destination...");

        let url = self.url.clone();
        let port = self.port;

        let server = Server::new(url.clone(), port);

        self.run_parameter = Some((pool.control_sender(), tx, id));

        let clone = self.clone();

        pool.execute_sync("TPC Destination".to_string(), move |meta| {
            server.start(id, clone, meta.output_channel, Arc::new(meta.ins.1))
        })
    }

    fn type_(&self) -> String {
        String::from("TPC")
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
