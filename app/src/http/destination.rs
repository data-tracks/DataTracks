use crate::http::util::{parse_addr, publish_ws, DestinationState, DEFAULT_URL};
use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::ui::ConfigModel;
use crate::util::{new_broadcast, new_id, HybridThreadPool, Rx};
use crate::util::Tx;
use axum::routing::get;
use axum::Router;
use serde_json::{Map, Value};
use std::collections::HashMap;
use tokio::net::TcpListener;
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
    pub fn new<S: AsRef<str>>(url: Option<S>, port: u16) -> Self {
        let sender = new_broadcast("Incoming HTTP Destination");
        HttpDestination {
            id: new_id(),
            url: url.map(|r| r.as_ref().to_string()).unwrap_or(DEFAULT_URL.to_string()),
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

async fn start_destination(http: HttpDestination, _rx: Rx<Command>, sender: Tx<Train>) {
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
        let url = options.get("url").map(|url| url.as_str()).flatten();
        let port = options
            .get("port")
            .map(|port| port.as_u64().unwrap() as u16)
            .unwrap();

        let destination = HttpDestination::new(url, port.to_owned());

        Ok(destination)
    }

    fn operate(
        &mut self,
        pool: HybridThreadPool,
    ) -> usize {
        let sender = self.sender.clone();

        let clone = self.clone();

        pool.execute_async("HTTP Destination", move |meta| {
            Box::pin(async move {
                start_destination(clone, meta.ins.1, sender).await;
            })
        }, vec![])

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
