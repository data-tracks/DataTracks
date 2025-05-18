use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use axum::Router;
use axum::routing::{get};
use crossbeam::channel::{unbounded, Receiver, Sender};
use serde_json::{Map, Value};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::cors::CorsLayer;
use tracing::{debug};
use crate::http::util::{parse_addr, publish_ws};
use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::{Train};
use crate::ui::ConfigModel;
use crate::util::{new_channel, Rx, Tx};

#[derive(Clone)]
pub struct HttpDestination {
    id: usize,
    url: String,
    port: u16,
    receiver: Rx<Train>,
    sender: Tx<Train>,
}

impl HttpDestination {
    pub fn new(url: String, port: u16) -> Self {
        let (sender, _num, receiver) = new_channel();
        HttpDestination{
            id: 0,
            url,
            port,
            receiver,
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

#[derive(Clone)]
pub(crate) struct DestinationState {
    pub rx: Arc<Mutex<Rx<Train>>>,
}

async fn start_destination(http: HttpDestination, _rx: Receiver<Command>, receiver: Rx<Train>){
    debug!("starting http destination on {url}:{port}...", url=http.url, port=http.port);
    let addr = parse_addr(http.url, http.port);

    let state = DestinationState {
        rx: Arc::new(Mutex::new(receiver)),
    };

    let app = Router::new()
        .route("/ws", get(publish_ws))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

impl Destination for HttpDestination {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized
    {
        let url = options.get("url").unwrap().as_str().unwrap();
        let port = options.get("port").unwrap().as_str().unwrap().parse::<u16>().unwrap();

        let destination = HttpDestination::new(url.to_string(), port.to_owned());

        Ok(destination)
    }

    fn operate(&mut self, _control: Arc<Sender<Command>>) -> Sender<Command> {
        let rt = Runtime::new().unwrap();

        let (tx, rx) = unbounded();
        let receiver = self.receiver.clone();

        let clone = self.clone();

        thread::spawn(move || {
            rt.block_on(async {
                start_destination(clone, rx, receiver).await;
            });
        });

        tx
    }

    fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }

    fn get_id(&self) -> usize {
        self.id
    }

    fn serialize(&self) -> DestinationModel {
        let mut configs = HashMap::new();
        configs.insert("url".to_string(), ConfigModel::text(&self.url.clone()));
        configs.insert("port".to_string(), ConfigModel::number(self.port.into()));
        DestinationModel { type_name: self.name(), id: self.id.to_string(), configs }
    }

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized,
    {
        None
    }
}