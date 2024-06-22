use std::path::PathBuf;

use axum::{Json, Router};
use axum::handler::HandlerWithoutStateExt;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::services::ServeDir;
use tracing::{debug, info};

pub fn start() {
    // Create a new Tokio runtime
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        startup().await;
    })
}

pub async fn startup() {
    info!("initializing router...");

    // We could also read our port in from the environment as well
    let assets_path = std::env::current_dir().unwrap();
    let port = 2666_u16;
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    let serve_dir = ServeDir::new(format!("{}/ui/dist", assets_path.to_str().unwrap()))
        .fallback(fallback_handler.into_service());

    let app = Router::new()
        .route("/html", get(html))
        .route("/json", get(json))
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


