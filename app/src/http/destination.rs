use crate::http::util::{parse_addr, publish_ws, DestinationState, DestinationTrainState};
use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::ui::ConfigModel;
use crate::util::new_broadcast;
use crate::util::{new_channel, Rx, Tx};
use axum::routing::get;
use axum::Router;
use crossbeam::channel::{unbounded, Receiver, Sender};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::cors::CorsLayer;
use tracing::{debug, error};

#[derive(Clone)]
pub struct HttpDestination {
    id: usize,
    url: String,
    port: u16,
    sender: Tx<Train>,
}

impl HttpDestination {
    pub fn new(url: String, port: u16) -> Self {
        let sender = new_broadcast("Incoming HTTP Destination");
        HttpDestination {
            id: 0,
            url,
            port,
            sender,
        }
    }
}

impl Configurable for HttpDestination {
    fn name(&self) -> String {
        "Http".to_string()
    }

    fn options(&self) -> Map<String, Value> {
        let mut options = Map::new();
        options.insert(String::from("url"), Value::String(self.url.clone()));
        options.insert(String::from("port"), Value::Number(self.port.into()));
        options
    }
}

async fn start_destination(http: HttpDestination, _rx: Receiver<Command>, sender: Tx<Train>) {
    debug!(
        "starting http destination on {url}:{port}...",
        url = http.url,
        port = http.port
    );
    let addr = match parse_addr(http.url, http.port) {
        Ok(addr) => addr,
        Err(err) => panic!("{}", err),
    };

    let state = DestinationState::train(String::from("HTTP Destination"), sender);

    let app = Router::new()
        .route("/ws", get(publish_ws))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(err) => panic!("{}", err),
    };
    match axum::serve(listener, app).await {
        Ok(_) => {}
        Err(err) => error!("{}", err),
    }
}

impl Destination for HttpDestination {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        let url = match options.get("url") {
            None => panic!("missing url"),
            Some(url) => url,
        }
        .as_str()
        .unwrap();
        let port = options
            .get("port")
            .unwrap()
            .to_string()
            .parse::<u16>()
            .unwrap();

        let destination = HttpDestination::new(url.to_string(), port.to_owned());

        Ok(destination)
    }

    fn operate(&mut self, _control: Arc<Sender<Command>>) -> Sender<Command> {
        let rt = Runtime::new().unwrap();

        let (tx, rx) = unbounded();
        let sender = self.sender.clone();

        let clone = self.clone();

        let res = thread::Builder::new()
            .name("HTTP Destination".to_string())
            .spawn(move || {
                rt.block_on(async {
                    start_destination(clone, rx, sender).await;
                });
            });
        match res {
            Ok(_) => {}
            Err(err) => error!("{}", err),
        }

        tx
    }

    fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }

    fn id(&self) -> usize {
        self.id
    }

    fn type_(&self) -> String {
        String::from("HTTP")
    }

    fn serialize(&self) -> DestinationModel {
        let mut configs = HashMap::new();
        configs.insert("url".to_string(), ConfigModel::text(&self.url.clone()));
        configs.insert("port".to_string(), ConfigModel::number(self.port.into()));
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
