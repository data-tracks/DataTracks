use axum::Router;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast::Sender;
use tokio::task::JoinSet;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tracing::{debug, error, info};
use util::Event;

struct EventState {
    sender: Arc<Sender<Event>>,
}
pub async fn start(joins: &mut JoinSet<()>, tx: Sender<Event>) {
    joins.spawn(async move {
        let root_dir = std::env::current_dir().unwrap();
        let dist_path = root_dir
            .join("dashboard")
            .join("dist")
            .join("dashboard")
            .join("browser");

        info!("{:?}", dist_path);

        let shared_state = Arc::new(EventState {
            sender: Arc::new(tx),
        });

        let app = Router::new()
            .route("/events", get(ws_event_handler))
            .route("/queues", get(ws_queue_handler))
            .layer(CorsLayer::permissive())
            .with_state(shared_state)
            .fallback_service(ServeDir::new(dist_path));

        let listener = TcpListener::bind("127.0.0.1:3131").await.unwrap();
        println!("Server running on http://127.0.0.1:3131");
        axum::serve(listener, app).await.unwrap();
    });
}

async fn ws_event_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<EventState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_event_socket(socket, state))
}

async fn handle_event_socket(mut socket: WebSocket, state: Arc<EventState>) {
    debug!("connected");
    let mut rx = state.sender.subscribe();

    while let Ok(event) = rx.recv().await {
        match event {
            Event::Queue(_) => {}
            e => {
                if let Ok(msg) = serde_json::to_string(&e)
                    && socket
                        .send(Message::Text(Utf8Bytes::from(msg)))
                        .await
                        .is_err()
                {
                    // Client disconnected
                    error!("disconnected event");
                    break;
                }
            }
        }
    }
}

async fn ws_queue_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<EventState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_queue_socket(socket, state))
}

async fn handle_queue_socket(mut socket: WebSocket, state: Arc<EventState>) {
    debug!("connected");
    let mut rx = state.sender.subscribe();

    while let Ok(event) = rx.recv().await {
        if let Event::Queue(q) = event
            && let Ok(msg) = serde_json::to_string(&q)
            && socket
                .send(Message::Text(Utf8Bytes::from(msg)))
                .await
                .is_err()
        {
            // Client disconnected
            error!("disconnected queue");
            break;
        }
    }
}
