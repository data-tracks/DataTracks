use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::management::Storage;
use crate::mqtt::MqttSource;
use crate::processing::destination::Destination;
use crate::processing::source::Source;
use crate::processing::{DebugDestination, HttpSource, Plan};
use crate::ui::ConfigModel;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, Response, StatusCode};
use axum::response::{IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use include_dir::{include_dir, Dir};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::cors::CorsLayer;
use tracing::{debug, info};

/*curl --header "Content-Type: application/json" --request POST --json '{"name":"wordcount","plan":"0--1{sql|SELECT * FROM $0}--2\nIn\nHttp{\"url\": \"localhost\", \"port\": \"3666\"}:0\nOut\nHttp{\"url\": \"localhost\", \"port\": \"4666\"}:2"}' http://localhost:2666/plans/create*/
/*curl --header "Content-Type: application/json" --request POST --json '{"name":"wordcount"}' http://localhost:2666/plans/start*/


// Embed the entire directory
static ASSETS_DIR: Dir<'_> = include_dir!("ui/dist");

pub fn start_web(storage: Arc<Mutex<Storage>>) {
    // Create a new Tokio runtime
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        startup(storage).await;
        debug!("Startup done.")
    })
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

pub async fn startup(storage: Arc<Mutex<Storage>>) {
    info!("initializing router...");

    // We could also read our port in from the environment as well
    let port = 2666_u16;
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));


    let state = WebState { storage };

    let app = Router::new()
        .route("/plans", get(get_plans))
        .route("/plans/create", post(create_plan))
        .route("/plans/stop", post(stop_plan))
        .route("/plans/start", post(start_plan))
        .route("/inouts/create", post(create_in_outs))
        .route("/options", get(get_options))
        .route("/status", get(get_status))
        .route("/{*path}", get(|path: Path<String>| serve_embedded_file(path.to_string())))
        .route("/", get(|| serve_embedded_file(String::from("/"))))
        .with_state(state)
        .layer(CorsLayer::permissive());
    //.nest_service("/", serve_dir);

    let listener = TcpListener::bind(&addr).await.unwrap();
    debug!("router initialized, now listening on port {}", port);
    info!("DataTracks started: http://localhost:{}", port);
    axum::serve(listener, app).await.unwrap();
    debug!("Finished serving.")
}

async fn get_plans(State(state): State<WebState>) -> impl IntoResponse {
    let plans = state.storage.lock().unwrap().plans.lock().unwrap().values().map(|plan| serde_json::to_value(plan).unwrap()).collect::<Value>();
    let msg = json!( {"plans": &plans});
    Json(msg)
}

async fn get_options(State(_state): State<WebState>) -> impl IntoResponse {
    let sources = vec![HttpSource::serialize_default().unwrap(), MqttSource::serialize_default().unwrap()];
    let destinations = vec![DebugDestination::serialize_default().unwrap()];
    let msg = json!( {"sources": &sources, "destinations": &destinations});
    Json(msg)
}

async fn get_status(State(_state): State<WebState>) -> impl IntoResponse {
    let msg = json!( {"status": "connected"});
    Json(msg)
}

async fn create_plan(State(state): State<WebState>, Json(payload): Json<CreatePlanPayload>) -> impl IntoResponse {
    debug!("{:?}", payload);

    let plan = Plan::parse(payload.plan.as_str());

    let mut plan = match plan {
        Ok(plan) => plan,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()),
    };

    plan.set_name(payload.name);
    state.storage.lock().unwrap().add_plan(plan);

    // Return a response
    (StatusCode::OK, "Plan created".to_string())
}

async fn start_plan(State(state): State<WebState>, Json(payload): Json<PlanPayload>) -> impl IntoResponse {
    debug!("{:?}", payload);

    state.storage.lock().unwrap().start_plan_by_name(payload.name);

    // Return a response
    (StatusCode::OK, "Plan started".to_string())
}

async fn stop_plan(State(_state): State<WebState>, Json(payload): Json<PlanPayload>) -> impl IntoResponse {
    debug!("{:?}", payload);

    // Return a response
    (StatusCode::FORBIDDEN, "Plan cannot be stooped yet.".to_string())
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
            <MqttSource as Source>::from(payload.configs)
        }
        "http" => {
            <HttpSource as Source>::from(payload.configs)
        }
        _ => {
            return Err("Unknown source".to_string());
        }
    };

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
    plan_id: usize,
    stop_id: usize,
    type_name: String,
    category: String,
    configs: HashMap<String, ConfigModel>,
}

#[derive(Clone)]
struct WebState {
    pub storage: Arc<Mutex<Storage>>,
}


#[derive(Deserialize, Debug)]
struct PlanPayload {
    name: String,
}
