use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::{Json, Router};
use axum::extract::State;
use axum::handler::HandlerWithoutStateExt;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize, Serializer};
use serde::ser::SerializeStruct;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tracing::{debug, info};

use crate::mangagement::Storage;
use crate::processing::Plan;

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
        .route("/html", get(html))
        .route("/json", get(json))
        .route("/plans", get(get_plans))
        .route("/plans/create", post(create_plan))
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

async fn html() -> impl IntoResponse {
    Html("<h1>Hello, World!</h1>")
}

async fn json() -> impl IntoResponse {
    let data = json!({
        "message": "Hello, this is your data!",
        "status": "success"
    });
    Json(data)
}

async fn get_plans(State(mut state): State<WebState>) -> impl IntoResponse {
    let plans = state.storage.lock().unwrap().plans.lock().unwrap().values().into_iter().map(|plan| serde_json::to_value(&plan).unwrap()).collect::<Value>();
    let msg = json!( {"plans": &plans});
    Json(msg)
}

async fn create_plan(State(mut state): State<WebState>, Json(payload): Json<CreatePlanPayload>) -> impl IntoResponse {
    println!("{:?}", payload);

    let plan = Plan::parse(payload.plan.as_str());
    state.storage.lock().unwrap().add_plan(plan);

    // Return a response
    (StatusCode::OK, "Plan created".to_string())
}

#[derive(Deserialize, Debug)]
struct CreatePlanPayload {
    name: String,
    plan: String,
}

#[derive(Clone)]
struct WebState {
    pub storage: Arc<Mutex<Storage>>,
}

/*const DUMMY: Value= json!({"plans":[{
    "name": "Plan Simple",
    "lines": {
        "0": {
            "num": 0,
            "stops": [0, 1, 3]
        },
        "1": {
            "num": 1,
            "stops": [4, 1]
        },
        "2": {
            "num": 2,
            "stops": [5, 1]
        },
        "3": {
            "num": 3,
            "stops": [6, 7]
        }
    },
    "stops": {
        "0": {
            "num": 0,
            "inputs": [8]
        },
        "1": {
            "num": 1,
            "transform": {
                "language": "SQL",
                "query": "SELECT * FROM $1, $4, $5"
            }
        },
        "3": {
            "num": 3,
            "outputs": [4]
        },
        "4": {
            "num": 4
        },
        "5": {
            "num": 5
        },
        "6": {
            "num": 6
        },
        "7": {
            "num": 7
        }
    }
}]});*/

