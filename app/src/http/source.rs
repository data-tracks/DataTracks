use crate::http::util::{parse_addr, receive, receive_ws, DEFAULT_URL};
use crate::processing::Train;
use crate::util::HybridThreadPool;
use axum::routing::{get, post};
use axum::Router;
use core::ConfigModel;
use core::Configurable;
use core::Source;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use threading::multi::MultiSender;
use threading::pool::WorkerMeta;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::log::debug;
use threading::command::Command::Ready;

// ws: npx wscat -c ws://127.0.0.1:3666/ws/data
// messages like: curl --json '{"website": "linuxize.com"}' localhost:5555/data/isabel
#[derive(Clone)]
pub struct HttpSource {
    url: String,
    port: u16,
}

impl HttpSource {
    pub(crate) fn new<S: AsRef<str>>(url: Option<S>, port: u16) -> Self {
        HttpSource {
            url: url
                .map(|r| r.as_ref().to_string())
                .unwrap_or(DEFAULT_URL.to_string()),
            port,
        }
    }

    pub fn get_default_configs() -> HashMap<String, ConfigModel> {
        HashMap::new()
    }
}

async fn start_source(
    outs: MultiSender<Train>,
    http: HttpSource,
    meta: WorkerMeta,
    id: usize,
) -> Result<(), String> {
    debug!(
        "starting http source on {url}:{port}...",
        url = http.url,
        port = http.port
    );

    let meta_probe = meta.clone();
    // Combine all shutdown signals using tokio::select!
    let server_shutdown_future = async move {
        loop {
            if meta_probe.should_stop() {
                break
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };

    let addr = match parse_addr(http.url, http.port) {
        Ok(addr) => addr,
        Err(err) => panic!("{}", err),
    };

    let state = SourceState {
        source: Arc::new(Mutex::new(outs)),
    };

    let app = Router::new()
        .route("/data", post(receive))
        .route("/ws", get(receive_ws))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = match TcpListener::bind(&addr).await {
        Ok(msg) => msg,
        Err(err) => panic!("failed to bind to {addr}: {err}"),
    };
    meta.output_channel.send(Ready(id))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(server_shutdown_future)
        .await
        .map_err(|err| format!("failed to start HTTP server: {}", err))
}

impl Configurable for HttpSource {
    fn name(&self) -> String {
        String::from("Http")
    }

    fn options(&self) -> Map<String, Value> {
        let mut options = Map::new();
        options.insert(String::from("url"), Value::String(self.url.clone()));
        options.insert(String::from("port"), Value::Number(self.port.into()));
        options
    }
}

impl TryFrom<HashMap<String, ConfigModel>> for HttpSource {
    type Error = String;

    fn try_from(configs: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        let port = match configs.get("port") {
            Some(port) => match port {
                ConfigModel::String(port) => port.string.parse::<u16>().unwrap(),
                ConfigModel::Number(port) => port.number as u16,
                _ => return Err(String::from("Could not create HttpSource.")),
            },
            _ => return Err(String::from("Could not create HttpSource.")),
        };
        let url = match configs.get("url") {
            Some(ConfigModel::String(url)) => Some(url.string.clone()),
            _ => None,
        };
        Ok(HttpSource::new(url, port))
    }
}

impl TryFrom<Map<String, Value>> for HttpSource {
    type Error = String;

    fn try_from(options: Map<String, Value>) -> Result<Self, Self::Error> {
        Ok(HttpSource::new(
            options.get("url").and_then(|url| url.as_str()),
            options
                .get("port")
                .map(|port| {
                    port.as_str()
                        .ok_or("Could not parse port.")
                        .map(|v| v.parse::<u16>().map_err(|_err| "Could not parse error"))?
                })
                .ok_or(format!("Did not provide necessary port {options:?}."))??,
        ))
    }
}

impl Source for HttpSource {
    fn operate(
        &mut self,
        id: usize,
        outs: MultiSender<Train>,
        pool: HybridThreadPool,
    ) -> Result<usize, String> {
        let clone = self.clone();

        pool.execute_async("HTTP Source", move |meta| {
            Box::pin(async move { start_source(outs, clone, meta, id).await })
        })
    }

    fn type_(&self) -> String {
        String::from("HTTP")
    }

    fn get_configs(&self) -> HashMap<String, ConfigModel> {
        HashMap::new()
    }
}

#[derive(Clone)]
pub struct SourceState {
    pub source: Arc<Mutex<MultiSender<Train>>>,
}
