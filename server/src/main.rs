use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::signal;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let address = std::env::var("ADDRESS").unwrap_or_else(|_| String::from("127.0.0.1"));
    let port = std::env::var("PORT").unwrap_or_else(|_| String::from("8000"));
    let addr: SocketAddr = format!("{address}:{port}")
        .parse()
        .expect("ADDRESS and PORT must form a valid socket address");

    let shutdown = CancellationToken::new();
    let app = cooklang_sync_server::create_server_with_shutdown(shutdown.clone()).await;

    let listener = TcpListener::bind(addr).await.expect("bind address");
    tracing::info!("listening on {}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            signal::ctrl_c().await.ok();
            shutdown.cancel();
        })
        .await
        .expect("server error");
}
