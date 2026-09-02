//! Integration tests for the chunk transport wire contract.
//!
//! `Remote::download_batch` in the client crate scans a download response for
//! `--<boundary>` and `X-Chunk-ID:` lines by hand rather than running it
//! through a multipart parser, so the exact bytes matter to an unmodified
//! client. Before the port those bytes came from `rocket_multipart`; they now
//! come from `chunks::response::MultipartMixed`, and these tests pin them.
//!
//! Everything here drives the real `chunks::router()` over a `tower::Service`
//! call. Chunk routes touch only the filesystem, never the DB pool, so no
//! sqlite file or migrations are needed.

use std::path::PathBuf;

use tokio::sync::{Mutex, MutexGuard};

use axum::body::Body;
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::json;
use tower::ServiceExt;

const JWT_SECRET: &str = "chunks-wire-test-secret";

/// Boundary used for the requests these tests send. Unrelated to the boundary
/// the server generates for its own download responses, which is random.
const REQUEST_BOUNDARY: &str = "chunkswiretestboundary";

/// `JWT_SECRET`, `UPLOAD_DIR` and `SYNC_ENFORCEMENT` are process-wide and are
/// read fresh on every request, so tests in this file take turns holding this
/// lock rather than racing each other's `set_var`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

async fn setup() -> MutexGuard<'static, ()> {
    let guard = ENV_LOCK.lock().await;

    std::env::set_var("JWT_SECRET", JWT_SECRET);
    std::env::set_var("UPLOAD_DIR", upload_dir());
    std::env::remove_var("SYNC_ENFORCEMENT");
    std::fs::create_dir_all(upload_dir()).expect("upload dir");

    guard
}

fn upload_dir() -> PathBuf {
    std::env::temp_dir().join(format!("cooklang-sync-chunks-wire-{}", std::process::id()))
}

/// The sharded location `ChunkId::file_path` derives from an id: the first two
/// characters become directory levels.
fn stored_path(id: &str) -> PathBuf {
    upload_dir().join(&id[0..1]).join(&id[1..2]).join(id)
}

fn token() -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;

    encode(
        &Header::new(Algorithm::HS256),
        &json!({ "uid": 1, "exp": exp }),
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )
    .unwrap()
}

fn authed(method: &str, uri: &str) -> axum::http::request::Builder {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", format!("Bearer {}", token()))
}

struct Res {
    status: StatusCode,
    content_type: String,
    body: Vec<u8>,
}

impl Res {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    fn boundary(&self) -> String {
        self.content_type
            .strip_prefix("multipart/mixed; boundary=")
            .expect("a multipart/mixed content type")
            .to_owned()
    }
}

async fn call(request: Request<Body>) -> Res {
    let response = cooklang_sync_server::chunks::router()
        .oneshot(request)
        .await
        .expect("router must answer");

    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .map(|value| value.to_str().expect("ascii content type").to_owned())
        .unwrap_or_default();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body must be readable")
        .to_bytes()
        .to_vec();

    Res {
        status,
        content_type,
        body,
    }
}

fn multipart_body(fields: &[(&str, &str)]) -> String {
    let mut body = String::new();

    for (name, contents) in fields {
        body.push_str(&format!(
            "--{REQUEST_BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{contents}\r\n"
        ));
    }
    body.push_str(&format!("--{REQUEST_BOUNDARY}--\r\n"));

    body
}

async fn upload(uri: &str, fields: &[(&str, &str)]) -> Res {
    call(
        authed("POST", uri)
            .header(
                CONTENT_TYPE,
                format!("multipart/form-data; boundary={REQUEST_BOUNDARY}"),
            )
            .body(Body::from(multipart_body(fields)))
            .unwrap(),
    )
    .await
}

async fn download(form_body: &str) -> Res {
    call(
        authed("POST", "/chunks/download")
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(form_body.to_owned()))
            .unwrap(),
    )
    .await
}

async fn retrieve(id: &str) -> Res {
    call(
        authed("GET", &format!("/chunks/{id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

#[tokio::test]
async fn an_uploaded_chunk_is_stored_sharded_and_served_back_byte_for_byte() {
    let _guard = setup().await;
    let id = "wireuploadone";
    let contents = "peer a wrote this\nwith two lines\n";
    std::fs::remove_file(stored_path(id)).ok();

    let uploaded = upload("/chunks/upload", &[(id, contents)]).await;

    assert_eq!(uploaded.status, StatusCode::OK);
    assert!(
        uploaded.body.is_empty(),
        "a successful upload answers with an empty body"
    );
    assert!(
        stored_path(id).exists(),
        "chunk must land under <upload>/<first char>/<second char>/<id>"
    );

    let fetched = retrieve(id).await;

    assert_eq!(fetched.status, StatusCode::OK);
    assert_eq!(fetched.content_type, "text/plain; charset=utf-8");
    assert_eq!(fetched.text(), contents);
}

#[tokio::test]
async fn both_deprecated_upload_routes_store_chunks_like_the_current_one() {
    let _guard = setup().await;

    for (route, id) in [
        ("/chunks", "wiredeprecateda"),
        ("/chunks/", "wiredeprecatedb"),
    ] {
        let contents = format!("stored via {route}");
        std::fs::remove_file(stored_path(id)).ok();

        let uploaded = upload(route, &[(id, &contents)]).await;

        assert_eq!(uploaded.status, StatusCode::OK, "route {route}");
        assert_eq!(retrieve(id).await.text(), contents, "route {route}");
    }
}

#[tokio::test]
async fn a_download_frames_every_chunk_exactly_as_the_client_parser_expects() {
    let _guard = setup().await;
    let (first, second) = ("wiredownloadaa", "wiredownloadbb");
    upload(
        "/chunks/upload",
        &[(first, "first body"), (second, "second body")],
    )
    .await;

    let response = download(&format!("chunk_ids[]={first}&chunk_ids[]={second}")).await;
    let boundary = response.boundary();

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(boundary.len(), 15, "boundary length is part of the contract");
    assert!(boundary.chars().all(|c| c.is_ascii_alphanumeric()));
    assert_eq!(
        response.text(),
        format!(
            "\r\n--{boundary}\r\nContent-Type: text/plain; charset=utf-8\r\nX-Chunk-ID: {first}\r\n\r\nfirst body\
             \r\n--{boundary}\r\nContent-Type: text/plain; charset=utf-8\r\nX-Chunk-ID: {second}\r\n\r\nsecond body\
             \r\n--{boundary}--\r\n"
        )
    );
}

#[tokio::test]
async fn every_form_value_is_taken_as_a_chunk_id_whatever_its_key() {
    let _guard = setup().await;
    let id = "wireanykeyaa";
    upload("/chunks/upload", &[(id, "any key body")]).await;

    let mut framings = Vec::new();
    for form_body in [
        format!("chunk_ids[]={id}"),
        format!("chunk_ids={id}"),
        format!("0={id}"),
    ] {
        let response = download(&form_body).await;
        assert_eq!(response.status, StatusCode::OK, "form body {form_body}");

        let boundary = response.boundary();
        framings.push(response.text().replace(&boundary, "BOUNDARY"));
    }

    assert!(framings[0].contains(&format!("X-Chunk-ID: {id}")));
    assert_eq!(framings[0], framings[1], "`chunk_ids[]=` and `chunk_ids=`");
    assert_eq!(framings[1], framings[2], "`chunk_ids=` and a numeric key");
}

#[tokio::test]
async fn an_upload_field_with_an_empty_name_is_skipped_but_still_succeeds() {
    let _guard = setup().await;

    let uploaded = upload("/chunks/upload", &[("", "content with no chunk id")]).await;

    assert_eq!(uploaded.status, StatusCode::OK);
    assert!(
        !upload_dir().join("null").exists(),
        "an empty id must not be written to the `null` shard"
    );
}

#[tokio::test]
async fn a_chunk_id_that_is_not_alphanumeric_is_rejected_with_422() {
    let _guard = setup().await;

    assert_eq!(
        retrieve("not-alphanumeric").await.status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn an_unknown_chunk_is_404() {
    let _guard = setup().await;

    assert_eq!(retrieve("wiremissingchunk").await.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_upload_without_a_multipart_content_type_is_404() {
    let _guard = setup().await;

    let response = call(
        authed("POST", "/chunks/upload")
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("wirechunk=body"))
            .unwrap(),
    )
    .await;

    assert_eq!(response.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_download_without_a_form_content_type_is_404() {
    let _guard = setup().await;

    let response = call(
        authed("POST", "/chunks/download")
            .header(CONTENT_TYPE, "text/plain")
            .body(Body::from("chunk_ids[]=wirechunk"))
            .unwrap(),
    )
    .await;

    assert_eq!(response.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_get_on_a_post_only_route_is_404_rather_than_405() {
    let _guard = setup().await;

    let response = call(authed("GET", "/chunks").body(Body::empty()).unwrap()).await;

    assert_eq!(response.status, StatusCode::NOT_FOUND);
    assert_is_error_page(&response, 404, "Not Found");
}

/// Asserts a response carries the default error page for `code`.
///
/// The exact template is pinned by the unit tests next to the renderer; what
/// this checks is that the renderer is actually *wired* onto a real route, and
/// the byte length makes that check exact rather than approximate.
fn assert_is_error_page(response: &Res, code: u16, reason: &str) {
    let expected_length = match code {
        404 => 435,
        413 => 477,
        415 => 501,
        422 => 496,
        500 => 488,
        other => panic!("no page is served for {other}"),
    };

    assert_eq!(response.status.as_u16(), code);
    assert_eq!(response.content_type, "text/html; charset=utf-8");
    assert_eq!(response.body.len(), expected_length);
    assert!(
        response.text().contains(&format!("<h1>{code}: {reason}</h1>")),
        "page for {code} reads: {}",
        response.text()
    );
}

#[tokio::test]
async fn an_unrouted_path_is_answered_with_the_default_404_page() {
    let _guard = setup().await;

    let response = call(authed("GET", "/no-such-route").body(Body::empty()).unwrap()).await;

    assert_is_error_page(&response, 404, "Not Found");
}

#[tokio::test]
async fn a_rejected_chunk_id_is_answered_with_the_default_422_page() {
    let _guard = setup().await;

    let response = retrieve("not-alphanumeric").await;

    assert_is_error_page(&response, 422, "Unprocessable Entity");
}

#[tokio::test]
async fn an_authentication_failure_keeps_its_json_body_rather_than_a_page() {
    let _guard = setup().await;

    let response = call(
        Request::builder()
            .method("GET")
            .uri("/chunks/wirechunk")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
    assert_eq!(response.content_type, "application/json");
    assert_eq!(response.text(), r#"{"error":"unauthorized"}"#);
}

#[tokio::test]
async fn an_upload_past_the_data_form_limit_fails_instead_of_being_stored() {
    let _guard = setup().await;
    let id = "wireoversize";
    std::fs::remove_file(stored_path(id)).ok();

    let oversize = "a".repeat(6 * 1024 * 1024);
    let response = upload("/chunks/upload", &[(id, &oversize)]).await;

    assert_is_error_page(&response, 500, "Internal Server Error");
    assert!(
        !stored_path(id).exists(),
        "a rejected upload must not leave a partial chunk behind"
    );
}

#[tokio::test]
async fn an_upload_just_under_the_data_form_limit_is_stored_whole() {
    let _guard = setup().await;
    let id = "wireatlimit";
    std::fs::remove_file(stored_path(id)).ok();

    let contents = "b".repeat(4 * 1024 * 1024);
    let response = upload("/chunks/upload", &[(id, &contents)]).await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(
        std::fs::read_to_string(stored_path(id)).expect("stored chunk"),
        contents
    );
}
