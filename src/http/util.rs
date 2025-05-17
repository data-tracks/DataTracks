use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::ptr::dangling;
use axum::extract::{Path, State, WebSocketUpgrade};
use axum::extract::ws::{Message, WebSocket};
use axum::http::StatusCode;
use axum::{Json};
use axum::response::{IntoResponse, Response};
use serde_json::{json, Map, Value};
use tracing::{debug, warn};
use crate::http::destination::DestinationState;
use crate::http::source::SourceState;
use crate::processing::Train;
use crate::value;
use crate::value::Dict;

pub async fn receive(State(state): State<SourceState>, Json(payload): Json<Value>) -> impl IntoResponse {
    debug!("New http message received: {:?}", payload);

    let value = transform_to_value(payload);
    let train = Train::new(vec![value::Value::Dict(value)]);

    for out in state.source.lock().unwrap().iter() {
        out.send(train.clone()).unwrap();
    }

    // Return a response
    (StatusCode::OK, "Done".to_string())
}

pub fn transform_to_value(payload: Value) -> Dict {
    match payload {
        Value::Object(o) => o.into(),
        v => {
            let mut map = BTreeMap::new();
            map.insert(String::from("$"), v.into());
            Dict::new(map)
        }
    }
}

pub async fn receive_with_topic(Path(topic): Path<String>, State(state): State<SourceState>, Json(payload): Json<Value>) -> impl IntoResponse {
    debug!("New http message received: {:?}", payload);

    let mut dict = transform_to_value(payload);
    dict.insert(String::from("topic"), value::Value::text(topic.as_str()));

    let train = Train::new(vec![value::Value::Dict(dict)]);
    for out in state.source.lock().unwrap().iter() {
        out.send(train.clone()).unwrap();
    }

    // Return a response
    (StatusCode::OK, "Done".to_string())
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

pub async fn parse_addr(url: String, port: u16) -> SocketAddr {
    // We could also read our port in from the environment as well
    let url = match &url {
        u if u.to_lowercase() == "localhost" => "127.0.0.1",
        u => u.as_str(),
    };

    match &url {
        url if url.parse::<IpAddr>().is_ok() => {
            format!("{url}:{port}", url = url, port = port)
                .parse::<SocketAddr>()
                .map_err(| e | format!("Failed to parse address: {}", e)).unwrap()
        }
        _ => {
            tokio::net::lookup_host(format!("{url}:{port}", url=url, port=port)).await.unwrap()
                .next()
                .ok_or("No valid addresses found").unwrap()
        }
    }
}


pub async fn publish_ws(ws: WebSocketUpgrade, State(state): State<DestinationState>) -> Response {
    ws.on_upgrade(|socket| handle_publish_socket(socket, state))
}

async fn handle_publish_socket(mut socket: WebSocket, state: DestinationState) {
    let rx = state.rx.lock().unwrap().clone();
    loop {
        if let Ok(train) = rx.recv() {
            match train.values {
                None => {}
                Some(values) => {
                    for value in values {
                        let value = match value {
                            value::Value::Wagon(w) => w.unwrap(),
                            value => value
                        };
                        match socket.send(Message::Text(serde_json::to_string(&value).unwrap().into())).await {
                            Ok(_) => {}
                            Err(err) => {
                                warn!("Failed to send message: {}", err);
                                return;
                            }
                        }
                    }
                }
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

                let value = if let Ok(payload) = serde_json::from_str::<Value>(&text) {
                    transform_to_value(payload)
                } else{
                    let value = json!({"d": *text});
                    transform_to_value(value.get("d").unwrap().clone())
                };
                let train = Train::new(vec![value::Value::Dict(value)]);

                debug!("New train created: {:?}", train);
                for out in state.source.lock().unwrap().iter_mut() {
                    if let Err(e) = out.send(train.clone()) {
                        debug!("Failed to send message: {:?}", e);
                    }
                }
            }
            _ => warn!("Error while reading from socket: {:?}", msg)
        }
    }
}