use crate::http::util::{DEFAULT_URL, DestinationState, parse_addr, publish_ws};
use crate::processing::Train;
use crate::processing::destination::Destination;
use crate::util::Tx;
use crate::util::{HybridThreadPool};
use axum::Router;
use axum::routing::get;
use core::ConfigModel;
use core::Configurable;
use core::StringModel;
use serde_json::{Map, Value};
use std::collections::HashMap;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::debug;
use error::error::TrackError;
use threading::command::Command::Ready;
use threading::pool::WorkerMeta;

#[derive(Clone)]
pub struct HttpDestination {
    url: String,
    port: u16,
}

impl HttpDestination {
    pub(crate) fn get_default_configs() -> HashMap<String, ConfigModel> {
        let mut map = HashMap::new();
        map.insert(
            String::from("url"),
            ConfigModel::String(StringModel::new(crate::mqtt::DEFAULT_URL)),
        );
        map.insert(
            String::from("port"),
            ConfigModel::String(StringModel::new("8989")),
        );
        map
    }
}

impl HttpDestination {
    pub fn new<S: AsRef<str>>(url: Option<S>, port: u16) -> Self {
        HttpDestination {
            url: url
                .map(|r| r.as_ref().to_string())
                .unwrap_or(DEFAULT_URL.to_string()),
            port,
        }
    }
}

impl TryFrom<HashMap<String, ConfigModel>> for HttpDestination {
    type Error = String;

    fn try_from(configs: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        let port = if let Some(port) = configs.get("port") {
            port.as_int()?
        } else {
            return Err(String::from("Could not create HttpDestination."));
        };
        let url = configs.get("url").map(|u| u.as_str());

        Ok(HttpDestination::new(url, port as u16))
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

async fn start_destination(
    http: HttpDestination,
    meta: WorkerMeta,
    rx: Tx<Train>,
    id: usize,
) -> Result<(), TrackError> {
    debug!(
        "starting http destination on {url}:{port}...",
        url = http.url,
        port = http.port
    );
    let addr = match parse_addr(http.url, http.port) {
        Ok(addr) => addr,
        Err(err) => panic!("{}", err),
    };

    let state = DestinationState::train(String::from("HTTP Destination"), rx);

    let app = Router::new()
        .route("/ws", get(publish_ws))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|err| err.to_string())?;
    meta.output_channel.send(Ready(id))?;
    axum::serve(listener, app)
        .await
        .map_err(|err| err.into())
}

impl Destination for HttpDestination {
    fn parse(options: Map<String, Value>) -> Result<Self, TrackError>
    where
        Self: Sized,
    {
        let url = options.get("url").and_then(|url| url.as_str());
        let port = options
            .get("port")
            .map(|port| port.as_u64().unwrap() as u16)
            .unwrap();

        let destination = HttpDestination::new(url, port.to_owned());

        Ok(destination)
    }

    fn operate(&mut self, id: usize, tx: Tx<Train>, pool: HybridThreadPool) -> Result<usize, TrackError> {
        let clone = self.clone();

        pool.execute_async(format!("HTTP Destination {}", id), move |meta| {
            Box::pin(async move { start_destination(clone, meta, tx, id).await })
        })
    }

    fn type_(&self) -> String {
        String::from("HTTP")
    }

    fn get_configs(&self) -> HashMap<String, ConfigModel> {
        let mut configs = HashMap::new();
        configs.insert("url".to_string(), ConfigModel::text(&self.url.clone()));
        configs.insert("port".to_string(), ConfigModel::number(self.port.into()));
        configs
    }
}
