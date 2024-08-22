use crate::mangagement::Storage;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::util::{Tx, GLOBAL_ID};
use crate::value;
use crate::value::Dict;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use crossbeam::channel::{unbounded, Sender};
use json::JsonValue;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::cors::CorsLayer;
use tracing::info;

pub struct HttpSource {
    id: i64,
    stop: i64,
    port: u16,
    outs: HashMap<i64, Tx<Train>>,
}

impl HttpSource {
    pub(crate) fn new(port: u16) -> Self {
        HttpSource { id: GLOBAL_ID.new_id(), stop: 0, port, outs: Default::default() }
    }


    async fn publish(State(state): State<SourceState>, Json(payload): Json<Value>) -> impl IntoResponse {
        println!("{:?}", payload);

        let train = Train::new(-1, vec![payload.into()]);

        for out in state.source.lock().unwrap().values() {
            out.send(train.clone()).unwrap();
        }

        // Return a response
        (StatusCode::OK, "Done".to_string())
    }

    async fn startup(&mut self) {
        info!("starting http source...");
        // We could also read our port in from the environment as well
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.port));

        let state = SourceState { source: Arc::new(Mutex::new(self.outs.clone())) };

        let app = Router::new()
            .route("/data", post(Self::publish))
            .layer(CorsLayer::permissive())
            .with_state(state);

        let listener = TcpListener::bind(&addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }
}


impl Source for HttpSource {
    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        let rt = Runtime::new().unwrap();

        let (tx, rx) = unbounded();
        rt.block_on(async {
            Self::startup(self).await;
        });
        tx
    }

    fn add_out(&mut self, id: i64, out: Tx<Train>) {
        self.outs.insert(id, out);
    }

    fn get_stop(&self) -> i64 {
        self.stop
    }

    fn get_id(&self) -> i64 {
        self.id
    }
}

#[derive(Clone)]
struct SourceState {
    pub source: Arc<Mutex<HashMap<i64, Tx<Train>>>>,
}

impl Into<Dict> for Value {
    fn into(self) -> Dict {
        let mut map = BTreeMap::new();
        map.insert("data".to_string(), value::Value::text(self.to_string().as_str()));
        Dict::new(map)
    }
}
