use crate::Event;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast::Sender;
use tokio::task::JoinSet;

// 1. Define the shared state for your metrics
struct AppState {
    sender: Arc<Sender<Event>>,
}

pub async fn start(joins: &mut JoinSet<()>, tx: Sender<Event>) {
    joins.spawn(async move {
        // Initialize state
        let shared_state = Arc::new(AppState {
            sender: Arc::new(tx),
        });

        // 2. Build the router
        let app = Router::new()
            //.route("/", get(root_handler))
            .route("/ws", get(ws_handler))
            .with_state(shared_state);

        // 3. Start the server
        let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
        println!("Server running on http://127.0.0.1:3000");
        axum::serve(listener, app).await.unwrap();
    });
}

// WebSocket handler
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.sender.subscribe();

    while let Ok(event) = rx.recv().await {

        if let Ok(msg) = serde_json::to_string(&event)&& socket.send(Message::Text(Utf8Bytes::from(msg))).await.is_err() {
            // Client disconnected
            break;
        }
    }
}