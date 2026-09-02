//! End-to-end tests for the peer-to-peer sync protocol: two clients of the
//! same account exchanging files through the server.
//!
//! These drive the whole application router (`create_server_with_shutdown`,
//! i.e. chunk routes merged with metadata routes) over a `tower::Service`
//! call, against a real sqlite database with the real diesel migrations
//! applied. They cover the parts of the port with no framework-independent
//! equivalent: the `rocket_sync_db_pools` replacement, the long-poll
//! notification fan-out, and the `Shutdown` request guard replacement.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::json;
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const JWT_SECRET: &str = "metadata-sync-test-secret";

/// `JWT_SECRET`, `UPLOAD_DIR`, `DATABASE_URL` and `SYNC_ENFORCEMENT` are
/// process-wide, and `DATABASE_URL` is read when the router is built, so tests
/// in this file take turns rather than racing each other's `set_var`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

async fn env_guard() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().await
}

fn upload_dir() -> PathBuf {
    std::env::temp_dir().join(format!("cooklang-sync-metadata-uploads-{}", std::process::id()))
}

/// Every test gets its own sqlite file, so records committed by one test can
/// never satisfy another test's assertions and record ids stay predictable.
fn fresh_database_url(test_name: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "cooklang-sync-metadata-{}-{test_name}.sqlite3",
        std::process::id()
    ));
    std::fs::remove_file(&path).ok();

    path.to_string_lossy().into_owned()
}

async fn app(test_name: &str) -> Router {
    app_with_shutdown(test_name, CancellationToken::new()).await
}

async fn app_with_shutdown(test_name: &str, shutdown: CancellationToken) -> Router {
    std::env::set_var("JWT_SECRET", JWT_SECRET);
    std::env::set_var("UPLOAD_DIR", upload_dir());
    std::env::set_var("DATABASE_URL", fresh_database_url(test_name));
    std::env::remove_var("SYNC_ENFORCEMENT");
    std::fs::create_dir_all(upload_dir()).expect("upload dir");

    cooklang_sync_server::create_server_with_shutdown(shutdown).await
}

/// Writes a chunk straight to the store. `commit` only records a file whose
/// chunks are already uploaded, so a peer must have put them there first.
fn place_chunk(id: &str, contents: &str) {
    let dir = upload_dir().join(&id[0..1]).join(&id[1..2]);
    std::fs::create_dir_all(&dir).expect("chunk dir");
    std::fs::write(dir.join(id), contents).expect("chunk file");
}

fn token(uid: i32) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;

    encode(
        &Header::new(Algorithm::HS256),
        &json!({ "uid": uid, "exp": exp }),
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )
    .unwrap()
}

fn authed(uid: i32, method: &str, uri: &str) -> axum::http::request::Builder {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", format!("Bearer {}", token(uid)))
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("router must answer");

    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body must be readable")
        .to_bytes()
        .to_vec();

    (status, String::from_utf8_lossy(&body).into_owned())
}

async fn commit(
    app: &Router,
    uid: i32,
    uuid: &str,
    path: &str,
    chunk_ids: &str,
    deleted: bool,
) -> (StatusCode, String) {
    send(
        app,
        authed(uid, "POST", &format!("/metadata/commit?uuid={uuid}"))
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(format!(
                "path={path}&deleted={deleted}&chunk_ids={chunk_ids}"
            )))
            .unwrap(),
    )
    .await
}

async fn list(app: &Router, uid: i32, jid: i32) -> (StatusCode, String) {
    send(
        app,
        authed(uid, "GET", &format!("/metadata/list?jid={jid}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

async fn has_files(app: &Router, uid: i32) -> (StatusCode, String) {
    send(
        app,
        authed(uid, "GET", "/metadata/has_files")
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

async fn poll(app: &Router, uid: i32, uuid: &str, seconds: u64) -> (StatusCode, String) {
    send(
        app,
        authed(
            uid,
            "GET",
            &format!("/metadata/poll?seconds={seconds}&uuid={uuid}"),
        )
        .body(Body::empty())
        .unwrap(),
    )
    .await
}

#[tokio::test]
async fn a_file_committed_by_one_peer_is_listed_for_the_other() {
    let _guard = env_guard().await;
    let app = app("p2p_list").await;
    place_chunk("aaaa1111", "chunk one");

    let (status, body) = commit(&app, 1001, "peer-a", "recipes/dinner.cook", "aaaa1111", false).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, r#"{"Success":1}"#);

    let (status, body) = list(&app, 1001, 0).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        r#"[{"id":1,"user_id":1001,"chunk_ids":"aaaa1111","deleted":false,"path":"recipes/dinner.cook"}]"#,
        "field order is part of the JSON contract the client deserializes"
    );
}

#[tokio::test]
async fn a_commit_naming_chunks_the_server_does_not_have_records_nothing() {
    let _guard = env_guard().await;
    let app = app("p2p_need_chunks").await;

    let (status, body) = commit(&app, 1002, "peer-a", "recipes/absent.cook", "zzzz9999", false).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, r#"{"NeedChunks":"zzzz9999"}"#);
    assert_eq!(
        list(&app, 1002, 0).await.1,
        "[]",
        "a NeedChunks answer must not have recorded the file"
    );
}

#[tokio::test]
async fn recommitting_identical_content_returns_the_existing_record_id() {
    let _guard = env_guard().await;
    let app = app("p2p_dedup").await;
    place_chunk("bbbb2222", "unchanged");

    let first = commit(&app, 1003, "peer-a", "recipes/same.cook", "bbbb2222", false).await;
    let second = commit(&app, 1003, "peer-a", "recipes/same.cook", "bbbb2222", false).await;

    assert_eq!(first.1, r#"{"Success":1}"#);
    assert_eq!(second.1, first.1, "a no-op commit must not insert a record");
    assert_eq!(
        list(&app, 1003, 0).await.1,
        r#"[{"id":1,"user_id":1003,"chunk_ids":"bbbb2222","deleted":false,"path":"recipes/same.cook"}]"#
    );
}

#[tokio::test]
async fn changing_a_file_supersedes_the_previous_record() {
    let _guard = env_guard().await;
    let app = app("p2p_supersede").await;
    place_chunk("dddd4444", "first version");
    place_chunk("eeee5555", "second version");

    let first = commit(&app, 1004, "peer-a", "recipes/edited.cook", "dddd4444", false).await;
    let second = commit(&app, 1004, "peer-a", "recipes/edited.cook", "eeee5555", false).await;

    assert_eq!(first.1, r#"{"Success":1}"#);
    assert_eq!(second.1, r#"{"Success":2}"#);
    assert_eq!(
        list(&app, 1004, 0).await.1,
        r#"[{"id":2,"user_id":1004,"chunk_ids":"eeee5555","deleted":false,"path":"recipes/edited.cook"}]"#,
        "only the latest record per path is listed"
    );
    assert_eq!(
        list(&app, 1004, 2).await.1,
        "[]",
        "a peer already at jid=2 has nothing new to fetch"
    );
}

#[tokio::test]
async fn has_files_turns_true_once_a_peer_commits_and_false_again_on_delete() {
    let _guard = env_guard().await;
    let app = app("p2p_has_files").await;
    place_chunk("ffff6666", "present");

    assert_eq!(has_files(&app, 1005).await.1, "false");

    commit(&app, 1005, "peer-a", "recipes/present.cook", "ffff6666", false).await;
    assert_eq!(has_files(&app, 1005).await.1, "true");

    commit(&app, 1005, "peer-a", "recipes/present.cook", "ffff6666", true).await;
    assert_eq!(
        has_files(&app, 1005).await.1,
        "false",
        "a deleted file must not count as a file"
    );
}

#[tokio::test]
async fn a_long_poll_is_released_early_by_another_peers_commit() {
    let _guard = env_guard().await;
    let app = app("p2p_poll_wake").await;
    place_chunk("cccc3333", "woken");

    let started = Instant::now();
    let (polled, committed) = tokio::join!(poll(&app, 1006, "peer-b", 30), async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        commit(&app, 1006, "peer-a", "recipes/woken.cook", "cccc3333", false).await
    });

    assert_eq!(committed.0, StatusCode::OK);
    assert_eq!(polled.0, StatusCode::OK);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the poll must be woken by the commit, not by its own 30s timeout"
    );
}

#[tokio::test]
async fn a_long_poll_returns_on_its_own_timeout_when_no_peer_commits() {
    let _guard = env_guard().await;
    let app = app("p2p_poll_timeout").await;

    let started = Instant::now();
    let (status, body) = poll(&app, 1007, "peer-solo", 1).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "the poll must actually wait out its requested window"
    );
}

#[tokio::test]
async fn an_in_flight_long_poll_is_released_when_the_server_shuts_down() {
    let _guard = env_guard().await;
    let shutdown = CancellationToken::new();
    let app = app_with_shutdown("p2p_poll_shutdown", shutdown.clone()).await;

    let started = Instant::now();
    let (polled, ()) = tokio::join!(poll(&app, 1008, "peer-c", 30), async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        shutdown.cancel();
    });

    assert_eq!(polled.0, StatusCode::OK);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "shutdown must release the poll rather than let it run its 30s course"
    );
}

#[tokio::test]
async fn a_commit_without_the_uuid_query_is_422() {
    let _guard = env_guard().await;
    let app = app("p2p_commit_no_uuid").await;

    let (status, _body) = send(
        &app,
        authed(1009, "POST", "/metadata/commit")
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("path=a.cook&deleted=false&chunk_ids=aaaa1111"))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn a_commit_missing_a_form_field_is_422() {
    let _guard = env_guard().await;
    let app = app("p2p_commit_bad_form").await;

    let (status, _body) = send(
        &app,
        authed(1010, "POST", "/metadata/commit?uuid=peer-a")
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("path=a.cook&chunk_ids=aaaa1111"))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn a_list_whose_jid_is_missing_or_not_an_integer_is_422() {
    let _guard = env_guard().await;
    let app = app("p2p_list_bad_jid").await;

    for uri in ["/metadata/list", "/metadata/list?jid=", "/metadata/list?jid=abc"] {
        let (status, _body) = send(
            &app,
            authed(1011, "GET", uri).body(Body::empty()).unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "uri {uri}");
    }
}

#[tokio::test]
async fn a_poll_without_its_query_parameters_is_422() {
    let _guard = env_guard().await;
    let app = app("p2p_poll_bad_query").await;

    for uri in ["/metadata/poll?uuid=peer-a", "/metadata/poll?seconds=1"] {
        let (status, _body) = send(
            &app,
            authed(1012, "GET", uri).body(Body::empty()).unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "uri {uri}");
    }
}

#[tokio::test]
async fn a_get_on_the_commit_route_is_404_rather_than_405() {
    let _guard = env_guard().await;
    let app = app("p2p_commit_method").await;

    let (status, _body) = send(
        &app,
        authed(1013, "GET", "/metadata/commit?uuid=peer-a")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}
