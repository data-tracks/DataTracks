use axum::Router;
use axum::extract::ws::Message::Binary;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Path, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tracing::{error, info, warn};
use util::{Batch, Event, StatisticEvent, TargetedRecord};

#[derive(Clone)]
struct EventState {
    sender: Sender<Event>,
    output: Sender<Batch<TargetedRecord>>,
    last: Arc<Mutex<StatisticEvent>>,
}
pub fn start(
    rt: &mut Runtime,
    tx: Sender<Event>,
    output: Sender<Batch<TargetedRecord>>,
    last: Arc<Mutex<StatisticEvent>>,
) {
    rt.spawn(async move {
        let root_dir = std::env::current_dir().unwrap();
        let dist_path = root_dir
            .join("dashboard")
            .join("dist")
            .join("dashboard")
            .join("browser");

        let shared_state = EventState {
            sender: tx,
            output,
            last,
        };

        let app = Router::new()
            .route("/events", get(ws_handler))
            .route("/queues", get(ws_handler))
            .route("/statistics", get(ws_handler))
            .route("/channel/{topic}", get(ws_channel_handler))
            .route("/threads", get(ws_handler))
            .layer(CorsLayer::permissive())
            .with_state(shared_state)
            .fallback_service(ServeDir::new(dist_path));

        let listener = TcpListener::bind("127.0.0.1:3131").await.unwrap();
        info!("Web server running on http://127.0.0.1:3131");
        axum::serve(listener, app).await.unwrap();
    });
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    path: axum::extract::MatchedPath,
    State(state): State<EventState>,
) -> impl IntoResponse {
    let path_str = path.as_str().to_string();
    ws.on_upgrade(move |socket| handle_socket_logic(socket, state, path_str))
}

async fn handle_socket_logic(mut socket: WebSocket, state: EventState, path: String) {
    let mut rx = state.sender.subscribe();

    if path.as_str() == "/statistics" {
        let statistics = (*state.last.lock().unwrap()).clone();
        let msg = serde_json::to_string(&Event::Statistics(statistics)).ok();
        if let Some(text) = msg
            && socket.send(Message::Text(text.into())).await.is_err()
        {
            error!("Error sending initial statistic.")
        }
    }

    loop {
        match rx.recv().await {
            Ok(event) => {
                // Centralized Filtering Logic
                let msg = match (path.as_str(), event) {
                    ("/events", e) => {
                        if matches!(e, Event::Heartbeat(_)) {
                            continue;
                        }
                        serde_json::to_string(&e).ok()
                    }
                    ("/queues", Event::Queue(q)) => serde_json::to_string(&q).ok(),
                    ("/statistics", e)
                        if matches!(e, Event::Statistics(_))
                            || matches!(e, Event::Throughput(_)) =>
                    {
                        serde_json::to_string(&e).ok()
                    }
                    ("/threads", Event::Heartbeat(h)) => Some(h),
                    _ => None,
                };

                if let Some(text) = msg
                    && socket.send(Message::Text(text.into())).await.is_err()
                {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("Client lagged by {} messages", n);
                continue;
            }
            Err(_) => break,
        }
    }
}

async fn ws_channel_handler(
    Path(id): Path<String>, // Extracts the ":id" from the URL
    ws: WebSocketUpgrade,
    State(state): State<EventState>,
) -> impl IntoResponse {
    info!("New connection to channel: {}", id);

    ws.on_upgrade(move |socket| handle_socket(socket, id, state))
}

async fn handle_socket(mut socket: WebSocket, topic: String, state: EventState) {
    let mut recv = state.output.subscribe();
    loop {
        if let Ok(msg) = recv.recv().await {
            let values = msg.into_iter().map(|msg| msg.value).collect::<Vec<_>>();
            let msg = value::message::Message {
                topics: vec![],
                payload: values,
                timestamp: 0,
            };
            if (socket).send(Binary(msg.pack().into())).await.is_err() {
                // Client disconnected
                error!("disconnected queue");
                return;
            }
        };
    }
}
