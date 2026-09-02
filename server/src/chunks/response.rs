use std::path::PathBuf;

use async_stream::stream;
use axum::body::{Body, Bytes};
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use rand::distr::Alphanumeric;
use rand::Rng;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

/// Header block emitted before each section's bytes. Reproduces byte for byte
/// what `rocket_multipart::MultipartStream` wrote, so `Remote::download_batch`
/// in the client -- which scans for `--<boundary>` and an `X-Chunk-ID:` line
/// itself rather than using a multipart parser -- keeps working unchanged.
const SECTION_CONTENT_TYPE: &str = "text/plain; charset=utf-8";
const BOUNDARY_LEN: usize = 15;

/// A `multipart/mixed` response streaming one section per requested chunk.
pub(crate) struct MultipartMixed {
    boundary: String,
    chunks: Vec<(String, PathBuf)>,
}

impl MultipartMixed {
    pub(crate) fn new_random(chunks: Vec<(String, PathBuf)>) -> Self {
        let boundary = rand::rng()
            .sample_iter(Alphanumeric)
            .map(char::from)
            .take(BOUNDARY_LEN)
            .collect();

        Self { boundary, chunks }
    }
}

impl IntoResponse for MultipartMixed {
    fn into_response(self) -> Response {
        let MultipartMixed { boundary, chunks } = self;
        let content_type = format!("multipart/mixed; boundary={boundary}");

        let body = Body::from_stream(stream! {
            for (id, file_path) in chunks {
                let file = File::open(file_path).await.expect("file present");

                yield Ok(Bytes::from(format!(
                    "\r\n--{boundary}\r\nContent-Type: {SECTION_CONTENT_TYPE}\r\nX-Chunk-ID: {id}\r\n\r\n"
                )));

                let mut contents = ReaderStream::new(file);
                while let Some(bytes) = contents.next().await {
                    yield bytes;
                }
            }

            yield Ok(Bytes::from(format!("\r\n--{boundary}--\r\n")));
        });

        ([(CONTENT_TYPE, content_type)], body).into_response()
    }
}
