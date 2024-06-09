use askama::Template;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::Router;
use axum::routing::get;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_http::services::ServeDir;
use tracing::info;
use tracing_subscriber::util::SubscriberInitExt;

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
    let router = Router::new()
        .route("/", get(hello))
        .nest_service(
            "/assets",
            ServeDir::new(format!("{}/web/assets", assets_path.to_str().unwrap())),
        );
    let listener = TcpListener::bind(&addr).await.unwrap();
    info!("router initialized, now listening on port {}", port);
    axum::serve(listener, router).await.unwrap();
}

async fn hello() -> impl IntoResponse {
    let template = HelloTemplate {};
    HtmlTemplate(template)
}

#[derive(Template)]
#[template(path = "hello.html")]
struct HelloTemplate;

/// A wrapper type that we'll use to encapsulate HTML parsed by askama into valid HTML for axum to serve.
struct HtmlTemplate<T>(T);

/// Allows us to convert Askama HTML templates into valid HTML for axum to serve in the response.
impl<T> IntoResponse for HtmlTemplate<T>
    where
        T: Template,
{
    fn into_response(self) -> Response {
        // Attempt to render the template with askama
        match self.0.render() {
            // If we're able to successfully parse and aggregate the template, serve it
            Ok(html) => Html(html).into_response(),
            // If we're not, return an error or some bit of fallback HTML
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to render template. Error: {}", err),
            )
                .into_response(),
        }
    }
}