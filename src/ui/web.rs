use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::handler::HandlerWithoutStateExt;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tracing::{debug, info};

use crate::management::Storage;
use crate::mqtt::MqttSource;
use crate::processing::destination::Destination;
use crate::processing::source::Source;
use crate::processing::{DebugDestination, HttpSource, Plan};
use crate::ui::ConfigModel;

pub fn start(storage: Arc<Mutex<Storage>>) {
    // Create a new Tokio runtime
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        startup(storage).await;
    })
}

pub async fn startup(storage: Arc<Mutex<Storage>>) {
    info!("initializing router...");

    // We could also read our port in from the environment as well
    let assets_path = std::env::current_dir().unwrap();
    let port = 2666_u16;
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    let serve_dir = ServeDir::new(format!("{}/ui/dist", assets_path.to_str().unwrap()))
        .fallback(fallback_handler.into_service());

    let state = WebState { storage };

    let app = Router::new()
        .route("/plans", get(get_plans))
        .route("/plans/create", post(create_plan))
        .route("/inouts/create", post(create_in_outs))
        .route("/options", get(get_options))
        .with_state(state)
        .layer(CorsLayer::permissive())
        .nest_service("/", serve_dir);

    let listener = TcpListener::bind(&addr).await.unwrap();
    debug!("router initialized, now listening on port {}", port);
    info!("DataTracks started: http://localhost:{}", port);
    axum::serve(listener, app).await.unwrap();
}

async fn fallback_handler() -> impl IntoResponse {
    let index_path = PathBuf::from("./ui/dist/index.html");
    match tokio::fs::read_to_string(index_path).await {
        Ok(contents) => Html(contents).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "500 Internal Server Error").into_response(),
    }
}

async fn get_plans(State(state): State<WebState>) -> impl IntoResponse {
    let plans = state.storage.lock().unwrap().plans.lock().unwrap().values().map(|plan| serde_json::to_value(plan).unwrap()).collect::<Value>();
    let msg = json!( {"plans": &plans});
    Json(msg)
}

async fn get_options(State(state): State<WebState>) -> impl IntoResponse {
    let sources = vec![HttpSource::serialize_default().unwrap(), MqttSource::serialize_default().unwrap()];
    let destinations = vec![DebugDestination::serialize_default().unwrap()];
    let msg = json!( {"sources": &sources, "destinations": &destinations});
    Json(msg)
}

async fn create_plan(State(state): State<WebState>, Json(payload): Json<CreatePlanPayload>) -> impl IntoResponse {
    debug!("{:?}", payload);

    let mut plan = Plan::parse(payload.plan.as_str());
    plan.set_name(payload.name);
    state.storage.lock().unwrap().add_plan(plan);

    // Return a response
    (StatusCode::OK, "Plan created".to_string())
}

async fn create_in_outs(State(state): State<WebState>, Json(payload): Json<CreateInOutsPayload>) -> impl IntoResponse {
    debug!("{:?}", payload);
    match payload.category.as_str() {
        "source" => {
            if let Err(value) = create_source(&state, payload) {
                return (StatusCode::BAD_REQUEST, value)
            }
        }
        "destination" => {
            if let Err(value) = create_destination(&state, payload) {
                return (StatusCode::BAD_REQUEST, value)
            }
        }
        _ => {}
    }

    // Return a response
    (StatusCode::OK, "Created".to_string())
}

fn create_source(state: &WebState, payload: CreateInOutsPayload) -> Result<(), String> {
    let source = match payload.type_name.to_lowercase().as_str() {
        "mqtt" => {
            <MqttSource as Source>::from(payload.stop_id, payload.configs)
        }
        "http" => {
            <HttpSource as Source>::from(payload.stop_id, payload.configs)
        }
        _ => {
            return Err("Unknown source".to_string());
        }
    };
    if let Err(err) = source {
        return Err(err);
    }

    state.storage.lock().unwrap().add_source(payload.plan_id, payload.stop_id, source?);
    Ok(())
}

fn create_destination(_state: &WebState, _payload: CreateInOutsPayload) -> Result<(), String> {
    Err("Unknown source".to_string())
}

#[derive(Deserialize, Debug)]
struct CreatePlanPayload {
    name: String,
    plan: String,
}

#[derive(Deserialize, Debug)]
struct CreateInOutsPayload {
    plan_id: i64,
    stop_id: i64,
    type_name: String,
    category: String,
    configs: HashMap<String, ConfigModel>,
}

#[derive(Clone)]
struct WebState {
    pub storage: Arc<Mutex<Storage>>,
}

