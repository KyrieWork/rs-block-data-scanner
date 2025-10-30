use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{Response, StatusCode, header},
    routing::get,
};
use clap::Parser;
use rs_block_data_scanner::{
    api::{
        handle_request,
        handlers::{ApiHttpResponse, get_key_value},
        init_logging,
        storage::ApiStorage,
    },
    cli::Cli,
    config::AppConfig,
};
use tokio::signal;
use tracing::{error, info};

async fn kv_handler(
    Path(key): Path<String>,
    State(storage): State<Arc<ApiStorage>>,
) -> Response<Body> {
    build_response(get_key_value(&key, &storage))
}

async fn fallback_handler(
    State(storage): State<Arc<ApiStorage>>,
    request: axum::http::Request<Body>,
) -> Response<Body> {
    let path = request.uri().path().to_string();
    build_response(handle_request(&path, &storage))
}

fn build_response(api_response: ApiHttpResponse) -> Response<Body> {
    Response::builder()
        .status(api_response.status)
        .header(header::CONTENT_TYPE, api_response.content_type)
        .body(Body::from(api_response.body))
        .unwrap_or_else(|err| {
            error!("Failed to build response: {err}");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{\"error\":\"Internal server error\"}"))
                .unwrap()
        })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let config = AppConfig::load(&args.config)?;

    init_logging(&config);

    let storage_path = format!(
        "{}/{}",
        config.storage.path.trim_end_matches('/'),
        config.scanner.chain_name
    );
    let storage = Arc::new(ApiStorage::open_readonly(&storage_path)?);

    let router = Router::new()
        .route("/kv/:key", get(kv_handler))
        .fallback(fallback_handler)
        .with_state(storage);

    let listen_addr: SocketAddr = "0.0.0.0:9001".parse()?;
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    info!("API service listening on {}", listener.local_addr()?);

    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(async {
            if signal::ctrl_c().await.is_ok() {
                info!("Shutdown signal received, stopping API service");
            }
        })
        .await?;

    Ok(())
}
