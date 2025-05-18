use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::ui::ConfigModel;
use crate::util::{new_id, Tx};
use crate::value;
use axum::routing::{get, post};
use axum::Router;
use crossbeam::channel::{unbounded, Receiver, Sender};
use serde_json::{Map, Value};
use std::collections::{HashMap};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::cors::CorsLayer;
use tracing::log::debug;
use crate::http::util::{parse_addr, receive, receive_with_topic, receive_ws};

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
        HttpSource { id: new_id(), url, port, outs: Default::default() }
    }

}


async fn start_source(http: HttpSource, _rx: Receiver<Command>){
    debug!("starting http source on {url}:{port}...", url=http.url, port=http.port);
    let addr = parse_addr(http.url, http.port);

    let state = SourceState { source: Arc::new(Mutex::new(http.outs.clone())) };

    let app = Router::new()
        .route("/data", post(receive))
        .route("/data/{*topic}", post(receive_with_topic))
        .route("/ws", get(receive_ws))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
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
            String::from(options.get("url").map(|url| url.as_str().ok_or("Could not parse url.")).ok_or(format!("Did not provide necessary url {:?}.", options))??),
            options.get("port").map(|port| port.as_str().ok_or("Could not parse port.").map(|v| v.parse::<u16>().map_err(|_err| "Could not parse error"))?).ok_or(format!("Did not provide necessary port {:?}.", options))??))
    }

    fn operate(&mut self, _control: Arc<Sender<Command>>) -> Sender<Command> {
        let rt = Runtime::new().unwrap();

        let (tx, rx) = unbounded();

        let clone = self.clone();

        thread::spawn(move || {
            rt.block_on(async {
                start_source(clone, rx).await;
            });
        });

        tx
    }

    fn outs(&mut self) -> &mut Vec<Tx<Train>> {
        &mut self.outs
    }

    fn id(&self) -> usize {
        self.id
    }


    fn serialize(&self) -> SourceModel {
        SourceModel { type_name: String::from("Http"), id: self.id.to_string(), configs: HashMap::new() }
    }

    fn from( configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String> {
        let port = match configs.get("port") {
            Some(port) => {
                match port {
                    ConfigModel::String(port) => {
                        port.string.parse::<u16>().unwrap()
                    }
                    ConfigModel::Number(port) => {
                        port.number as u16
                    }
                    _ => return Err(String::from("Could not create HttpSource."))
                }
            }
            _ => return Err(String::from("Could not create HttpSource."))
        };
        let url = match configs.get("url") {
            Some(ConfigModel::String(url)) => {
                url.string.clone()
            },
            _ => return Err(String::from("Could not create HttpSource."))
        };
        Ok(Box::new(HttpSource::new(url, port)))
    }

    fn serialize_default() -> Result<SourceModel, ()> {
        Ok(SourceModel { type_name: String::from("Http"), id: String::from("Http"), configs: HashMap::new() })
    }
}

#[derive(Clone)]
pub(crate) struct SourceState {
    pub source: Arc<Mutex<Vec<Tx<Train>>>>,
}

impl From<Value> for value::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => value::Value::null(),
            Value::Bool(b) => value::Value::bool(b),
            Value::Number(n) => {
                if n.is_f64() {
                    value::Value::float(n.as_f64().unwrap())
                } else {
                    value::Value::int(n.as_i64().unwrap())
                }
            }
            Value::String(s) => value::Value::text(&s),
            Value::Array(a) => {
                let mut values = vec![];
                for value in a {
                    values.push(value.into());
                }
                value::Value::array(values)
            }
            Value::Object(o) => {
                o.into()
            }
        }
    }
}

