use std::path::PathBuf;

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path};
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures::{Stream, StreamExt};
use multer::Multipart;
use tokio::fs::{self, create_dir_all, File};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use tower_http::catch_panic::CatchPanicLayer;
use tracing::{error, info};

use crate::auth::EntitledUser;
use crate::chunk_id::ChunkId;

mod request;
mod response;

use crate::chunks::request::RawContentType;
use crate::chunks::response::MultipartMixed;

const EMPTY_CHUNK_ID: ChunkId = ChunkId(std::borrow::Cow::Borrowed(""));

/// Body caps carried over from `rocket.toml` (`data-form = "5Mib"`,
/// `form = "1Mib"`), which is where Rocket's `Data::open(limits)` and form
/// parsing got them from.
const DATA_FORM_LIMIT: usize = 5 * 1024 * 1024;
const FORM_LIMIT: usize = 1024 * 1024;

const MULTIPART_FORM_DATA: &str = "multipart/form-data";
const URLENCODED_FORM: &str = "application/x-www-form-urlencoded";

/// Rocket refused to route a request whose `Content-Type` didn't match the
/// route's declared `format`, which surfaced as a 404. These handlers check
/// the header themselves to keep that same response.
enum ChunkError {
    UnmatchedFormat,
    Io(std::io::Error),
}

impl From<std::io::Error> for ChunkError {
    fn from(error: std::io::Error) -> Self {
        ChunkError::Io(error)
    }
}

impl IntoResponse for ChunkError {
    fn into_response(self) -> Response {
        match self {
            ChunkError::UnmatchedFormat => StatusCode::NOT_FOUND.into_response(),
            ChunkError::Io(error) => {
                error!("chunk storage error: {}", error);
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

/// Truncates a request body at `limit` bytes, ending the stream there.
///
/// Rocket enforced `data-form = "5Mib"` from `rocket.toml` by capping the
/// `Data` stream, so an oversize upload was cut short mid-field: the multipart
/// parser then hit end-of-input before the closing boundary, `field.chunk()`
/// failed, and the handler's panic became a 500. [`DefaultBodyLimit`] cannot
/// stand in for that on these two routes, as it only takes effect in extractors
/// that buffer the body and these handlers take the raw [`Body`] so they can
/// stream fields straight to disk. Truncating rather than erroring is what
/// keeps the failure on `field.chunk()`, where Rocket had it, instead of
/// surfacing it earlier as a parse error on the field itself.
fn capped_data_stream(
    body: Body,
    limit: usize,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> {
    let mut stream = body.into_data_stream();
    let mut seen = 0usize;

    async_stream::stream! {
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    let remaining = limit - seen;

                    if bytes.len() >= remaining {
                        yield Ok(bytes.slice(0..remaining));
                        return;
                    }

                    seen += bytes.len();

                    yield Ok(bytes);
                }
                Err(error) => {
                    yield Err(std::io::Error::other(error));
                    return;
                }
            }
        }
    }
}

async fn upload_chunks_deprecated(
    _user: EntitledUser,
    content_type: RawContentType,
    upload: Body,
) -> Result<(), ChunkError> {
    if !content_type.0.starts_with(MULTIPART_FORM_DATA) {
        return Err(ChunkError::UnmatchedFormat);
    }

    let boundary = multer::parse_boundary(content_type.0).unwrap();
    let mut multipart = Multipart::new(capped_data_stream(upload, DATA_FORM_LIMIT), boundary);

    while let Ok(Some(mut field)) = multipart.next_field().await {
        let field_name = field.name().unwrap();
        let chunk_id = ChunkId::from(field_name);

        if chunk_id == EMPTY_CHUNK_ID {
            continue;
        }

        let full_path = chunk_id.file_path();

        if let Some(parent) = full_path.parent() {
            create_dir_all(parent).await?;
        }

        let mut file = File::create(full_path.clone()).await?;

        while let Some(chunk) = match field.chunk().await {
            Ok(v) => v,
            Err(_e) => {
                fs::remove_file(&full_path).await.ok();

                // TODO
                panic!("Error reading chunk");
            }
        } {
            let _ = file.write_all(&chunk).await.map_err(|_| {
                std::fs::remove_file(&full_path).ok();
            });
        }
    }

    Ok(())
}

async fn upload_chunks(
    _user: EntitledUser,
    content_type: RawContentType,
    upload: Body,
) -> Result<(), ChunkError> {
    if !content_type.0.starts_with(MULTIPART_FORM_DATA) {
        return Err(ChunkError::UnmatchedFormat);
    }

    let boundary = multer::parse_boundary(content_type.0).unwrap();
    let mut multipart = Multipart::new(capped_data_stream(upload, DATA_FORM_LIMIT), boundary);

    while let Ok(Some(mut field)) = multipart.next_field().await {
        let field_name = field.name().unwrap();
        let chunk_id = ChunkId::from(field_name);

        if chunk_id == EMPTY_CHUNK_ID {
            continue;
        }

        let full_path = chunk_id.file_path();

        if let Some(parent) = full_path.parent() {
            create_dir_all(parent).await?;
        }

        let mut file = File::create(full_path.clone()).await?;

        while let Some(chunk) = match field.chunk().await {
            Ok(v) => v,
            Err(e) => {
                error!("Error reading chunk {:?}", e);
                fs::remove_file(&full_path).await.ok();

                // TODO
                panic!("Error reading chunk");
            }
        } {
            let _ = file.write_all(&chunk).await.map_err(|_| {
                std::fs::remove_file(&full_path).ok();
            });
        }
    }

    Ok(())
}

/// Downloads chunk from a storage
// TODO batch download
// TODO does it need to check that user can access chunk?
async fn retrieve(_user: EntitledUser, Path(id): Path<String>) -> Response {
    let Ok(id) = ChunkId::from_param(&id) else {
        return StatusCode::UNPROCESSABLE_ENTITY.into_response();
    };

    if id == EMPTY_CHUNK_ID {
        return StatusCode::NOT_FOUND.into_response();
    }

    match File::open(id.file_path()).await {
        Ok(file) => (
            [(CONTENT_TYPE, "text/plain; charset=utf-8")],
            Body::from_stream(ReaderStream::new(file)),
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Every value in the form body is taken as a chunk id, whatever its key.
/// Rocket's `FromForm` derive on the `ChunkIds(Vec<ChunkId>)` newtype behaved
/// this way, so the client's `chunk_ids[]=..` keys and any other spelling all
/// resolve to the same list.
fn requested_chunks(body: &str) -> Vec<(String, PathBuf)> {
    serde_urlencoded::from_str::<Vec<(String, String)>>(body)
        .unwrap_or_default()
        .iter()
        .map(|(_, value)| {
            let chunk_id = ChunkId::from(value.as_str());

            (chunk_id.id().to_string(), chunk_id.file_path())
        })
        .collect()
}

async fn download_chunks(
    _user: EntitledUser,
    content_type: RawContentType,
    chunk_ids: String,
) -> Result<MultipartMixed, ChunkError> {
    if !content_type.0.starts_with(URLENCODED_FORM) {
        return Err(ChunkError::UnmatchedFormat);
    }

    Ok(MultipartMixed::new_random(requested_chunks(&chunk_ids)))
}

pub fn router() -> Router {
    info!(
        "sync_entitlement_enforcement_mode={}",
        crate::auth::current_mode_name()
    );

    Router::new()
        .route("/chunks", post(upload_chunks_deprecated))
        .route("/chunks/", post(upload_chunks_deprecated))
        .route("/chunks/upload", post(upload_chunks))
        .route(
            "/chunks/download",
            post(download_chunks).layer(DefaultBodyLimit::max(FORM_LIMIT)),
        )
        .route("/chunks/{id}", get(retrieve))
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(DATA_FORM_LIMIT))
        .layer(CatchPanicLayer::new())
        .layer(axum::middleware::map_response(
            crate::error::default_error_pages,
        ))
}

/// Rocket had no 405: a request whose method matched no route simply found
/// no route and got a 404. Axum answers 405 by default, so it is mapped back.
async fn method_not_allowed() -> StatusCode {
    StatusCode::NOT_FOUND
}
