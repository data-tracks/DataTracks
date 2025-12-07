use core::models::configuration::ConfigModel;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::management::{Api, Storage};
use crate::processing::destination::{Destinations};
use crate::processing::Plan;
use crate::util::deserialize_message;
use axum::body::Body;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Path, State, WebSocketUpgrade};
use axum::http::{Response, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use include_dir::{Dir, include_dir};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info};
use track_rails::message_generated::protocol::Payload;
use error::error::TrackError;
use crate::processing::source::Sources;
/*curl --header "Content-Type: application/json" --request POST --json '{"name":"wordcount","plan":"0--1{sql|SELECT * FROM $0}--2\nIn\nHttp{\"url\": \"localhost\", \"port\": \"3666\"}:0\nOut\nHttp{\"url\": \"localhost\", \"port\": \"4666\"}:2"}' http://localhost:2666/plans/create*/
/*curl --header "Content-Type: application/json" --request POST --json '{"name":"wordcount"}' http://localhost:2666/plans/start*/

// Embed the entire directory
static ASSETS_DIR: Dir<'_> = include_dir!("./target/ui");

pub async fn start_web(storage: Storage) {
    startup(storage).await;
    debug!("Startup done.")
}

async fn serve_embedded_file(path: String) -> impl IntoResponse {
    debug!("Serve route {}", path);
    let path = if path.is_empty() || path == "/" {
        "index.html"
    } else {
        path.trim_start_matches('/')
    };

    match ASSETS_DIR.get_file(path) {
        Some(file) => {
            let mime_type = mime_guess::from_path(file.path()).first_or_octet_stream();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime_type.as_ref())
                .body(Body::from(file.contents()))
                .unwrap()
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
    }
}

pub async fn startup(storage: Storage) {
    debug!("initializing router...");

    // We could also read our port in from the environment as well
    let port = 2666_u16;
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));

    let state = WebState {
        storage,
        api: Arc::new(Mutex::new(Api::default())),
    };

    let app = Router::new()
        .route("/ws", get(websocket_handler))
        .route("/status", get(get_status))
        .route("/", get(|| serve_embedded_file(String::from("/"))))
        .with_state(state)
        .layer(CorsLayer::permissive());
    //.nest_service("/", serve_dir);

    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(error) => panic!("Unable to bind to {addr}: {error}"),
    };
    debug!("router initialized, now listening on port {}", port);
    info!("DataTracks (TrackView) started: http://localhost:{}", port);
    match axum::serve(listener, app).await {
        Ok(_) => {}
        Err(err) => error!("Error: {}", err),
    };
    debug!("Finished serving.")
}

// WebSocket handler
async fn websocket_handler(
    State(state): State<WebState>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(state, socket))
}

// Function to handle WebSocket communication
async fn handle_socket(state: WebState, mut socket: WebSocket) {
    debug!("Client connected!");

    while let Some(Ok(msg)) = socket.recv().await {
        match msg {
            Message::Text(text) => {
                info!("Received: {}", text);
            }
            Message::Binary(bin) => {
                let message = match deserialize_message(bin.as_ref()) {
                    Ok(msg) => msg,
                    Err(_) => continue,
                };
                info!("Received message: {:?}", message);

                if matches!(message.data_type(), Payload::Disconnect) {
                    info!("Disconnected from server");
                    return;
                }

                let _res =
                    match Api::handle_message(state.storage.clone(), state.api.clone(), message) {
                        Ok(msg) => socket.send(Message::from(msg)),
                        Err(err) => socket.send(Message::from(err)),
                    }
                    .await;
            }
            Message::Close(_) => {
                error!("Client disconnected");
                break;
            }
            _ => {}
        };
    }
}


async fn get_options(State(_state): State<WebState>) -> impl IntoResponse {
    let sources = Sources::get_default_configs();
    let destinations = Destinations::get_default_configs();
    let msg = json!( {"sources": &sources, "destinations": &destinations});
    Json(msg)
}

async fn get_status(State(_state): State<WebState>) -> impl IntoResponse {
    let msg = json!( {"status": "connected"});
    Json(msg)
}

async fn create_plan(
    State(state): State<WebState>,
    Json(payload): Json<CreatePlanPayload>,
) -> impl IntoResponse {
    debug!("{:?}", payload);

    let plan = Plan::parse(payload.plan.as_str());

    let mut plan = match plan {
        Ok(plan) => plan,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()),
    };

    plan.set_name(payload.name);
    todo!()
}


#[derive(Deserialize, Debug)]
struct CreatePlanPayload {
    name: String,
    plan: String,
}

#[derive(Deserialize, Debug)]
struct CreateInOutsPayload {
    plan_id: usize,
    stop_id: usize,
    type_name: String,
    category: String,
    configs: HashMap<String, ConfigModel>,
}

#[derive(Clone)]
struct WebState {
    pub storage: Storage,
    pub api: Arc<Mutex<Api>>,
}

#[derive(Deserialize, Debug)]
struct PlanPayload {
    name: String,
}
