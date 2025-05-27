use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::thread;
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
use value;
use crate::util::new_id;
use crate::util::new_channel;
use value::Dict;

pub async fn receive(State(state): State<SourceState>, Json(payload): Json<Value>) -> impl IntoResponse {
    debug!("New http message received: {:?}", payload);

    let train = transform_to_train(payload);

    for out in state.source.lock().unwrap().iter() {
        out.send(train.clone()).unwrap();
    }

    // Return a response
    (StatusCode::OK, "Done".to_string())
}

pub fn transform_to_train(payload: Value) -> Train {
    match payload {
        v => {
            match serde_json::from_str::<Train>(v.as_str().unwrap()) {
                Ok(msg) => msg,
                Err(_) => {
                    let mut map = BTreeMap::new();
                    map.insert(String::from("$"), v.into());
                    Train::new(vec![value::Value::Dict(Dict::new(map))])
                }
            }
        }
    }.mark(0)
}

pub async fn receive_with_topic(Path(topic): Path<String>, State(state): State<SourceState>, Json(payload): Json<Value>) -> impl IntoResponse {
    debug!("New http message received: {:?}", payload);

    let mut dict = transform_to_train(payload);
    //dict.insert(String::from("topic"), value::Value::text(topic.as_str()));
    todo!();
    //let train = Train::new(vec![value::Value::Dict(dict)]);
    for out in state.source.lock().unwrap().iter() {
        //out.send(train.clone()).unwrap();
    }

    // Return a response
    (StatusCode::OK, "Done".to_string())
}


pub fn parse_addr<S: AsRef<str>>(url: S, port: u16) -> SocketAddr {
    // We could read our port in from the environment as well
    let url = match url.as_ref() {
        u if u.to_lowercase() == "localhost" => "127.0.0.1",
        u => u,
    }.to_string();

    thread::Builder::new().name("Socket Address Lookup".to_string()).spawn(
        move || {
            let rt = tokio::runtime::Runtime::new().unwrap();

            rt.block_on(async {
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
            })
        }
    ).unwrap().join().unwrap()
    
}


pub async fn publish_ws(ws: WebSocketUpgrade, State(state): State<DestinationState>) -> Response {
    ws.on_upgrade(|socket| handle_publish_socket(socket, state))
}

async fn handle_publish_socket(mut socket: WebSocket, state: DestinationState) {
    let (tx, rx) = new_channel("");
    let id = new_id();
    {
        // drop after
        state.outs.lock().unwrap().insert(id, tx);
    }
    
    loop {
        match rx.recv() {
            Ok(train) => {
                match socket.send(Message::Text(serde_json::to_string(&train).unwrap().into())).await {
                    Ok(_) => {}
                    Err(err) => {
                        for _ in 0..3 {
                            match socket.send(Message::Text(serde_json::to_string(&train).unwrap().into())).await {
                                Ok(_) => { continue }
                                Err(_) => {}
                            }
                        }

                        warn!("Failed to send message after retry: {}", err);
                        state.outs.lock().unwrap().remove(&id);
                        return;
                    }
                }
            }
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
                } else{
                    let value = json!({"d": *text});
                    transform_to_train(text.parse().unwrap())
                };

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