use crate::http::source::SourceState;
use crate::processing::Train;
use crate::util::{Rx, Tx};
use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::Value;
use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::thread;
use tracing::{debug, warn};
use value;
use value::{Dict, Time};

pub async fn receive(
    State(state): State<SourceState>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    debug!("New http message received: {:?}", payload);

    let train = transform_to_train(payload);

    for out in state.source.lock().unwrap().iter() {
        out.send(train.clone());
    }

    // Return a response
    (StatusCode::OK, "Done".to_string())
}

pub fn transform_to_train(payload: Value) -> Train {
    let v = payload;
    match serde_json::from_str::<Train>(v.as_str().unwrap()) {
        Ok(msg) => msg,
        Err(_) => {
            let mut map = BTreeMap::new();
            map.insert(String::from("$"), v.into());
            Train::new(vec![value::Value::Dict(Dict::new(map))], 0)
        }
    }
    .mark(0)
}
pub fn parse_addr<S: AsRef<str>>(url: S, port: u16) -> Result<SocketAddr, String> {
    // We could read our port in from the environment as well
    let url = match url.as_ref() {
        u if u.to_lowercase() == "localhost" => "127.0.0.1",
        u => u,
    }
    .to_string();

    let result = thread::Builder::new()
        .name("Socket Address Lookup".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();

            rt.block_on(async {
                match &url {
                    url if url.parse::<IpAddr>().is_ok() => {
                        format!("{url}:{port}", url = url, port = port)
                            .parse::<SocketAddr>()
                            .map_err(|e| format!("Failed to parse address: {}", e))
                            .unwrap()
                    }
                    _ => tokio::net::lookup_host(format!("{url}:{port}", url = url, port = port))
                        .await
                        .unwrap()
                        .next()
                        .ok_or("No valid addresses found")
                        .unwrap(),
                }
            })
        })
        .unwrap()
        .join();

    match result {
        Ok(socket_addr) => Ok(socket_addr),
        Err(err) => Err(format!("Failed to parse socket Address: {:?}", err)),
    }
}

#[derive(Clone)]
pub enum DestinationState {
    Train(DestinationTrainState),
    Time(DestinationTimeState),
}

impl DestinationState {
    fn name(&self) -> String {
        match self {
            DestinationState::Train(t) => t.name.clone(),
            DestinationState::Time(t) => t.name.clone(),
        }
    }
}

pub enum RxWrapper {
    Train(Rx<Train>),
    Time(Rx<Time>),
}

impl RxWrapper {
    pub(crate) fn recv(&self) -> Result<Utf8Bytes, String> {
        match self {
            RxWrapper::Train(t) => t.recv().map(|t| serde_json::to_string(&t).unwrap().into()),
            RxWrapper::Time(t) => t.recv().map(|t| {
                serde_json::to_string(&Train::new(vec![t.into()], 0))
                    .unwrap()
                    .into()
            }),
        }
    }
}

impl DestinationState {
    pub fn train<S: AsRef<str>>(name: S, receiver: Tx<Train>) -> DestinationState {
        let name = name.as_ref().to_string();
        DestinationState::Train(DestinationTrainState {
            name,
            out: receiver,
        })
    }

    pub fn time<S: AsRef<str>>(name: S, receiver: Tx<Time>) -> DestinationState {
        let name = name.as_ref().to_string();
        DestinationState::Time(DestinationTimeState {
            name,
            out: receiver,
        })
    }
    fn subscribe(&self) -> RxWrapper {
        match self {
            DestinationState::Train(t) => RxWrapper::Train(t.out.subscribe()),
            DestinationState::Time(t) => RxWrapper::Time(t.out.subscribe()),
        }
    }
}

#[derive(Clone)]
pub struct DestinationTrainState {
    pub name: String,
    pub out: Tx<Train>,
}

#[derive(Clone)]
pub struct DestinationTimeState {
    pub name: String,
    pub out: Tx<Time>,
}

pub async fn publish_ws(ws: WebSocketUpgrade, State(state): State<DestinationState>) -> Response {
    ws.on_upgrade(|socket| handle_publish_socket(socket, state))
}

async fn handle_publish_socket(mut socket: WebSocket, state: DestinationState) {
    let rx = state.subscribe();
    let name = state.name();
    loop {
        match rx.recv() {
            Ok(item) => match socket.send(Message::Text(item.clone())).await {
                Ok(_) => {}
                Err(err) => {
                    for _ in 0..3 {
                        if socket.send(Message::Text(item.clone())).await.is_ok() {
                            continue;
                        }
                    }

                    warn!("Failed to send message after retry: {} in {}", err, name);

                    return;
                }
            },
            Err(err) => {
                warn!("Error {err}")
            }
        }
    }
}

pub async fn receive_ws(ws: WebSocketUpgrade, State(state): State<SourceState>) -> Response {
    ws.on_upgrade(|socket| handle_receive_socket(socket, state))
}

async fn handle_receive_socket(mut socket: WebSocket, state: SourceState) {
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!("New http message received: {:?}", text);

                let train = if let Ok(payload) = serde_json::from_str::<Value>(&text) {
                    transform_to_train(payload)
                } else {
                    transform_to_train(text.parse().unwrap())
                };

                debug!("New train created: {:?}", train);
                for out in state.source.lock().unwrap().iter_mut() {
                    out.send(train.clone());
                }
            }
            _ => warn!("Error while reading from socket: {:?}", msg),
        }
    }
}
