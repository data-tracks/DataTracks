use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::ui::ConfigModel;
use crate::util::{Tx, GLOBAL_ID};
use crate::value;
use crate::value::Dict;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use crossbeam::channel::{unbounded, Receiver, Sender};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::cors::CorsLayer;
use tracing::{debug, info};

// messages like: curl --json '{"website": "linuxize.com"}' localhost:5555/data/isabel
#[derive(Clone)]
pub struct HttpSource {
    id: i64,
    url: String,
    port: u16,
    outs: Vec<Tx<Train>>,
}

impl HttpSource {
    pub(crate) fn new(url: String, port: u16) -> Self {
        HttpSource { id: GLOBAL_ID.new_id(), url, port, outs: Default::default() }
    }


    async fn publish(State(state): State<SourceState>, Json(payload): Json<Value>) -> impl IntoResponse {
        debug!("New http message received: {:?}", payload);

        let value = Self::transform_to_value(payload);
        let train = Train::new(-1, vec![value::Value::Dict(value)]);

        for out in state.source.lock().unwrap().iter() {
            out.send(train.clone()).unwrap();
        }

        // Return a response
        (StatusCode::OK, "Done".to_string())
    }

    fn transform_to_value(payload: Value) -> Dict {
        match payload {
            Value::Object(o) => o.into(),
            v => {
                let mut map = BTreeMap::new();
                map.insert(String::from("data"), v.into());
                Dict::new(map)
            }
        }
    }

    async fn publish_with_topic(Path(topic): Path<String>, State(state): State<SourceState>, Json(payload): Json<Value>) -> impl IntoResponse {
        debug!("New http message received: {:?}", payload);

        let mut dict = Self::transform_to_value(payload);
        dict.insert(String::from("topic"), value::Value::text(topic.as_str()));

        let train = Train::new(-1, vec![value::Value::Dict(dict)]);
        for out in state.source.lock().unwrap().iter() {
            out.send(train.clone()).unwrap();
        }


        // Return a response
        (StatusCode::OK, "Done".to_string())
    }

    async fn startup(self, _rx: Receiver<Command>) {
        info!("starting http source...");
        // We could also read our port in from the environment as well
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.port));

        let state = SourceState { source: Arc::new(Mutex::new(self.outs.clone())) };

        let app = Router::new()
            .route("/data", post(Self::publish))
            .route("/data/*topic", post(Self::publish_with_topic))
            .layer(CorsLayer::permissive())
            .with_state(state);

        let listener = TcpListener::bind(&addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }
}

impl Configurable for HttpSource {
    fn get_name(&self) -> String {
        String::from("Http")
    }

    fn get_options(&self) -> Map<String, Value> {
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
        Ok(HttpSource::new(String::from(options.get("url").unwrap().as_str().unwrap()), options.get("port").unwrap().as_u64().unwrap() as u16))
    }

    fn operate(&mut self, _control: Arc<Sender<Command>>) -> Sender<Command> {
        let rt = Runtime::new().unwrap();

        let (tx, rx) = unbounded();
        let clone = self.clone();
        thread::spawn(move || {
            rt.block_on(async {
                Self::startup(clone, rx).await;
            });
        });

        tx
    }

    fn get_outs(&mut self) -> &mut Vec<Tx<Train>> {
        &mut self.outs
    }

    fn get_id(&self) -> i64 {
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
struct SourceState {
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

impl From<Map<String, Value>> for value::Value {
    fn from(value: Map<String, Value>) -> Self {
        value::Value::Dict(value.into())
    }
}

impl From<Map<String, Value>> for Dict {
    fn from(value: Map<String, Value>) -> Self {
        let mut map = BTreeMap::new();
        for (key, value) in value {
            map.insert(key, value.into());
        }
        Dict::new(map)
    }
}
