use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use tower_http::catch_panic::CatchPanicLayer;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::auth::user::User;
use crate::auth::EntitledUser;
use crate::error::AppError;
use crate::extract::Query;

mod db;
mod middleware;
mod models;
mod notification;
mod request;
mod response;
mod schema;

use db::{has_files as db_has_files, insert_new_record, latest_for_path, list as db_list, Db};
use models::{FileRecord, NewFileRecord};

use notification::ActiveClients;

type Result<T, E = AppError> = std::result::Result<T, E>;

/// Default from `rocket.toml`'s `[default.databases.metadata] url`, now read
/// from the environment since there is no framework-managed config file.
const DEFAULT_DATABASE_URL: &str = "db/server.sqlite3";

/// Carried over from `rocket.toml`'s `form = "1Mib"` limit.
const FORM_LIMIT: usize = 1024 * 1024;

#[derive(Clone)]
struct MetadataState {
    db: Db,
    clients: Arc<Mutex<ActiveClients>>,
    shutdown: CancellationToken,
}

#[derive(Deserialize)]
struct CommitQuery {
    uuid: String,
}

#[derive(Deserialize)]
struct ListQuery {
    jid: i32,
}

#[derive(Deserialize)]
struct PollQuery {
    seconds: u64,
    uuid: String,
}

// check if all hashes are present
// if any not present return back need more and list of hashes
// if present all insert into db path and chunk hashes and return back a new jid
async fn commit(
    user: EntitledUser,
    State(state): State<MetadataState>,
    Query(query): Query<CommitQuery>,
    Form(commit_payload): Form<request::CommitPayload>,
) -> Result<Json<response::CommitResultStatus>> {
    let to_be_uploaded = commit_payload.non_local_chunks();

    match to_be_uploaded.is_empty() {
        true => {
            let r = NewFileRecord::from_payload_and_user_id(commit_payload, user.id);

            // Dedup: if the latest record for (user_id, path) already has the
            // same chunk_ids and deleted flag, this commit is a no-op. Return
            // the existing id without inserting or notifying other clients.
            // Guards against buggy / outdated clients that re-commit unchanged
            // files in a loop.
            let existing = {
                let user_id = r.user_id;
                let path = r.path.clone();
                state
                    .db
                    .run(move |conn| latest_for_path(conn, user_id, &path))
                    .await?
            };

            if let Some(existing) = existing {
                if existing.chunk_ids == r.chunk_ids && existing.deleted == r.deleted {
                    tracing::info!(
                        "dedup: no-op commit user_id={} path={:?} existing_id={}",
                        r.user_id,
                        r.path,
                        existing.id
                    );
                    return Ok(Json(response::CommitResultStatus::Success(existing.id)));
                }
            }

            let id: i32 = state.db.run(move |conn| insert_new_record(conn, r)).await?;

            state.clients.lock().unwrap().notify(&query.uuid);

            Ok(Json(response::CommitResultStatus::Success(id)))
        }
        false => {
            let to_be_uploaded_strings: Vec<String> = to_be_uploaded
                .iter()
                .map(|chunk_id| chunk_id.0.to_string())
                .collect();

            Ok(Json(response::CommitResultStatus::NeedChunks(
                to_be_uploaded_strings.join(","),
            )))
        }
    }
}

async fn has_files(user: User, State(state): State<MetadataState>) -> Result<Json<bool>> {
    let result = state.db.run(move |conn| db_has_files(conn, user.id)).await?;

    Ok(Json(result))
}

// return back array of jid, path, hashes for all jid since requested
async fn list(
    user: EntitledUser,
    State(state): State<MetadataState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Vec<FileRecord>>> {
    let records = state
        .db
        .run(move |conn| db_list(conn, user.id, query.jid))
        .await?;

    Ok(Json(records))
}

async fn poll(
    _user: EntitledUser,
    State(state): State<MetadataState>,
    Query(query): Query<PollQuery>,
) -> Result<()> {
    let seconds = notification::clamp_poll_seconds(query.seconds);

    let notification = state.clients.lock().unwrap().register(&query.uuid);

    let timeout = tokio::time::timeout(Duration::from_secs(seconds), notification.notified());

    let result = tokio::select! {
        _ = state.shutdown.cancelled() => Ok(()),
        _ = timeout => Ok(()),
    };

    state.clients.lock().unwrap().remove(&query.uuid);

    result
}

pub async fn router(shutdown: CancellationToken) -> Router {
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_string());

    let db = Db::new(&database_url);
    middleware::run_migrations(&db).await;

    let state = MetadataState {
        db,
        clients: Arc::new(notification::init()),
        shutdown,
    };

    Router::new()
        .route("/metadata/commit", post(commit))
        .route("/metadata/has_files", get(has_files))
        .route("/metadata/list", get(list))
        .route("/metadata/poll", get(poll))
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(FORM_LIMIT))
        .layer(CatchPanicLayer::new())
        .layer(axum::middleware::map_response(
            crate::error::default_error_pages,
        ))
        .with_state(state)
}

async fn method_not_allowed() -> StatusCode {
    StatusCode::NOT_FOUND
}
