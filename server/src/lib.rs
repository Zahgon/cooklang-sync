#[cfg(all(feature = "database_sqlite", feature = "database_postgres"))]
compile_error!(
    "feature \"database_sqlite\" and feature \"database_postgres\" cannot be enabled at the same time"
);

extern crate diesel;

use axum::Router;
use tokio_util::sync::CancellationToken;

mod auth;
mod chunk_id;
pub mod chunks;
mod error;
mod extract;
pub mod metadata;

/// Builds the full application router. The returned router is never asked to
/// shut down, so `metadata::poll` only ever returns on its own timeout or on
/// a client notification. Binaries that want long polls to be released on
/// shutdown should use [`create_server_with_shutdown`] instead.
pub async fn create_server() -> Router {
    create_server_with_shutdown(CancellationToken::new()).await
}

/// Builds the full application router, wired to `shutdown` so in-flight long
/// polls are released when the token is cancelled. This replaces Rocket's
/// `Shutdown` request guard, which the `metadata::poll` handler used to
/// select on.
pub async fn create_server_with_shutdown(shutdown: CancellationToken) -> Router {
    chunks::router().merge(metadata::router(shutdown).await)
}
