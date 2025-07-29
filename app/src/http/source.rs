use crate::http::util::{parse_addr, receive, receive_ws, DEFAULT_URL};
use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Sources::Http;
use crate::processing::source::{Source, Sources};
use crate::processing::station::Command;
use crate::processing::Train;
use crate::ui::ConfigModel;
use crate::util::{new_id, HybridThreadPool};
use crate::util::Tx;
use axum::routing::{get, post};
use axum::Router;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::error;
use tracing::log::debug;
use crate::util::Rx;

// ws: npx wscat -c ws://127.0.0.1:3666/ws/data
// messages like: curl --json '{"website": "linuxize.com"}' localhost:5555/data/isabel
#[derive(Clone)]
pub struct HttpSource {
    id: usize,
    url: String,
    port: u16,
    outs: Vec<Tx<Train>>,
}

impl HttpSource {
    pub(crate) fn new<S: AsRef<str>>(url: Option<S>, port: u16) -> Self {
        HttpSource {
            id: new_id(),
            url: url.map(|r| r.as_ref().to_string()).unwrap_or(DEFAULT_URL.to_string()),
            port,
            outs: Default::default(),
        }
    }
}

async fn start_source(http: HttpSource, rx: Rx<Command>) {
    debug!(
        "starting http source on {url}:{port}...",
        url = http.url,
        port = http.port
    );

    // Combine all shutdown signals using tokio::select!
    let server_shutdown_future = async move {
        loop {
            match rx.try_recv() {
                Ok(Command::Stop(_)) => break,
                _ => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    };


    let addr = match parse_addr(http.url, http.port) {
        Ok(addr) => addr,
        Err(err) => panic!("{}", err),
    };

    let state = SourceState {
        source: Arc::new(Mutex::new(http.outs.clone())),
    };

    let app = Router::new()
        .route("/data", post(receive))
        .route("/ws", get(receive_ws))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = match TcpListener::bind(&addr).await {
        Ok(msg) => msg,
        Err(err) => panic!("failed to bind to {}: {}", addr, err),
    };
    match axum::serve(listener, app).with_graceful_shutdown(server_shutdown_future).await {
        Ok(_) => {}
        Err(err) => error!("failed to start HTTP server: {}", err),
    }
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

impl Source for HttpSource {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        Ok(HttpSource::new(
            options.get("url").map(|url| url.as_str()).flatten(),
            options
                .get("port")
                .map(|port| {
                    port.as_str()
                        .ok_or("Could not parse port.")
                        .map(|v| v.parse::<u16>().map_err(|_err| "Could not parse error"))?
                })
                .ok_or(format!("Did not provide necessary port {:?}.", options))??,
        ))
    }

    fn operate(
        &mut self,
        pool: HybridThreadPool,
    ) -> usize {
        let clone = self.clone();

        pool.execute_async("HTTP Source", move |meta| {
            Box::pin(async move {
                start_source(clone, meta.ins.1.clone()).await;
            })
        }, vec![])
    }

    fn outs(&mut self) -> &mut Vec<Tx<Train>> {
        &mut self.outs
    }

    fn id(&self) -> usize {
        self.id
    }

    fn type_(&self) -> String {
        String::from("HTTP")
    }

    fn serialize(&self) -> SourceModel {
        SourceModel {
            type_name: String::from("Http"),
            id: self.id.to_string(),
            configs: HashMap::new(),
        }
    }

    fn from(configs: HashMap<String, ConfigModel>) -> Result<Sources, String> {
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
        Ok(Http(HttpSource::new(url, port)))
    }

    fn serialize_default() -> Result<SourceModel, ()> {
        Ok(SourceModel {
            type_name: String::from("Http"),
            id: String::from("Http"),
            configs: HashMap::new(),
        })
    }
}

#[derive(Clone)]
pub struct SourceState {
    pub source: Arc<Mutex<Vec<Tx<Train>>>>,
}
