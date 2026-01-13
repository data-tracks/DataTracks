use axum::Router;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::broadcast::Sender;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tracing::{debug, error, info};
use util::{Event, StatisticEvent};

struct EventState {
    sender: Arc<Sender<Event>>,
}
pub fn start(rt: &mut Runtime, tx: Sender<Event>) {
    rt.spawn(async move {
        let root_dir = std::env::current_dir().unwrap();
        let dist_path = root_dir
            .join("dashboard")
            .join("dist")
            .join("dashboard")
            .join("browser");

        let shared_state = Arc::new(EventState {
            sender: Arc::new(tx),
        });

        let app = Router::new()
            .route("/events", get(ws_event_handler))
            .route("/queues", get(ws_queue_handler))
            .route("/statistics", get(ws_statistics_handler))
            .layer(CorsLayer::permissive())
            .with_state(shared_state)
            .fallback_service(ServeDir::new(dist_path));

        let listener = TcpListener::bind("127.0.0.1:3131").await.unwrap();
        info!("Web server running on http://127.0.0.1:3131");
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

    let mut events = vec![];
    loop {
        match rx.recv().await {
            Ok(event) => {
                events.push(event);

                while let Ok(event) = rx.try_recv() {
                    events.push(event);
                }

                for event in events.drain(..) {
                    match event {
                        Event::Queue(_) => {}
                        Event::Insert(..) => {}
                        e => {
                            if let Ok(msg) = serde_json::to_string(&e)
                                && socket
                                    .send(Message::Text(Utf8Bytes::from(msg)))
                                    .await
                                    .is_err()
                            {
                                // Client disconnected
                                error!("disconnected event");
                                return;
                            }
                        }
                    }
                }
            }
            Err(err) => {
                debug!("event: {}", err)
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

    let mut events = vec![];
    loop {
        match rx.recv().await {
            Ok(event) => {
                events.push(event);
                while let Ok(event) = rx.try_recv() {
                    events.push(event)
                }

                for event in events.drain(..) {
                    if let Event::Queue(q) = event
                        && let Ok(msg) = serde_json::to_string(&q)
                    {
                        if (socket)
                            .send(Message::Text(Utf8Bytes::from(msg)))
                            .await
                            .is_err()
                        {
                            // Client disconnected
                            error!("disconnected queue");
                            return;
                        }
                    } else {
                        // we ignore others
                    }
                }
            }
            Err(err) => {
                debug!("error: {}", err)
            }
        }
    }
}


async fn ws_statistics_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<EventState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_statistics_socket(socket, state))
}

async fn handle_statistics_socket(mut socket: WebSocket, state: Arc<EventState>) {
    debug!("connected");
    let mut rx = state.sender.subscribe();

    // send first empty statistics
    if let Ok(msg) = serde_json::to_string(&StatisticEvent{ engines: Default::default() })
    {
        if (socket)
            .send(Message::Text(Utf8Bytes::from(msg)))
            .await
            .is_err()
        {
            // Client disconnected
            error!("disconnected queue");
            return;
        }
    } else {
        // we ignore others
    }


    let mut events = vec![];
    loop {
        match rx.recv().await {
            Ok(event) => {
                events.push(event);
                while let Ok(event) = rx.try_recv() {
                    events.push(event)
                }

                for event in events.drain(..) {
                    if let Event::Statistics(q) = event
                        && let Ok(msg) = serde_json::to_string(&q)
                    {
                        if (socket)
                            .send(Message::Text(Utf8Bytes::from(msg)))
                            .await
                            .is_err()
                        {
                            // Client disconnected
                            error!("disconnected queue");
                            return;
                        }
                    } else {
                        // we ignore others
                    }
                }
            }
            Err(err) => {
                debug!("error: {}", err)
            }
        }
    }
}