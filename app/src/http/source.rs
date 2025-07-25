use crate::http::util::{parse_addr, receive, receive_ws};
use crate::processing::Train;
use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Sources::Http;
use crate::processing::source::{Source, Sources};
use crate::processing::station::Command;
use crate::ui::ConfigModel;
use crate::util::Tx;
use crate::util::new_id;
use axum::Router;
use axum::routing::{get, post};
use crossbeam::channel::{Receiver, Sender, unbounded};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::cors::CorsLayer;
use tracing::error;
use tracing::log::debug;

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
    pub(crate) fn new(url: String, port: u16) -> Self {
        HttpSource {
            id: new_id(),
            url,
            port,
            outs: Default::default(),
        }
    }
}

async fn start_source(http: HttpSource, _rx: Receiver<Command>) {
    debug!(
        "starting http source on {url}:{port}...",
        url = http.url,
        port = http.port
    );
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
    match axum::serve(listener, app).await {
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
            String::from(
                options
                    .get("url")
                    .map(|url| url.as_str().ok_or("Could not parse url."))
                    .ok_or(format!("Did not provide necessary url {:?}.", options))??,
            ),
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

    fn operate(&mut self, _control: Arc<Sender<Command>>) -> Sender<Command> {
        let rt = Runtime::new().unwrap();

        let (tx, rx) = unbounded();

        let clone = self.clone();

        let res = thread::Builder::new()
            .name("HTTP Source".to_string())
            .spawn(move || {
                rt.block_on(async {
                    start_source(clone, rx).await;
                });
            });

        match res {
            Ok(_) => {}
            Err(err) => error!("{}", err),
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
            Some(ConfigModel::String(url)) => url.string.clone(),
            _ => return Err(String::from("Could not create HttpSource.")),
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
