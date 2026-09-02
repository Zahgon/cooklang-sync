//! Integration tests for the `EntitledUser` extractor's actual Axum wiring:
//! that `chunks::router()` really does answer with the 401/402 JSON bodies,
//! and that the extractor's off/enforce decision really does gate the request
//! before the handler runs -- over a real `tower::Service` call.
//!
//! Deliberately uses only `chunks::router()` (no `metadata::router()`), since
//! `chunks` routes touch neither the DB pool nor the filesystem until
//! *after* the extractor has already let a request through, so these tests
//! need no sqlite file or migrations.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::{json, Value};
use tokio::sync::{Mutex, MutexGuard};
use tower::ServiceExt;

const JWT_SECRET: &str = "entitlement-catchers-test-secret";

/// `JWT_SECRET` and `SYNC_ENFORCEMENT` are process-wide env vars read fresh
/// on every request (see `auth::request::secret` and
/// `auth::entitlement::enforcement_mode`). This file is its own test
/// binary/process (cargo builds each `tests/*.rs` file separately), so this
/// lock only needs to serialize the handful of tests *within this file*
/// against each other -- it has no interaction with unit tests elsewhere in
/// the crate.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

async fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().await
}

fn far_future_exp() -> usize {
    (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600) as usize
}

fn sign(payload: &Value) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        payload,
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )
    .unwrap()
}

/// Sends a GET for a chunk that does not exist on disk through the real
/// `chunks::router()`, optionally carrying a bearer token, and returns the
/// status plus the raw body. `JWT_SECRET` must already be set by the caller,
/// since a request carrying a bearer token reads it.
async fn get_chunk(token: Option<&str>) -> (StatusCode, Vec<u8>) {
    let app = cooklang_sync_server::chunks::router();

    let mut request = Request::builder().uri("/chunks/doesnotexist12345");
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }

    let response = app
        .oneshot(request.body(Body::empty()).unwrap())
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

    (status, body)
}

fn as_json(body: &[u8]) -> Value {
    serde_json::from_slice(body).expect("rejection must return a JSON body")
}

#[tokio::test]
async fn request_without_a_token_is_rejected_with_401_and_the_contract_body() {
    let _guard = env_lock().await;
    std::env::set_var("JWT_SECRET", JWT_SECRET);
    std::env::remove_var("SYNC_ENFORCEMENT"); // mode is irrelevant: no token, no claim to judge

    let (status, body) = get_chunk(None).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(as_json(&body), json!({ "error": "unauthorized" }));
}

#[tokio::test]
async fn missing_entitlement_under_enforce_is_rejected_with_402_and_the_contract_body() {
    let _guard = env_lock().await;
    std::env::set_var("JWT_SECRET", JWT_SECRET);
    std::env::set_var("SYNC_ENFORCEMENT", "enforce");

    // Valid signature, valid (non-expired) token -- but no `sync_until`
    // claim at all, i.e. a non-entitled (or pre-entitlement) account.
    let token = sign(&json!({ "uid": 1, "exp": far_future_exp() }));

    let (status, body) = get_chunk(Some(&token)).await;

    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(
        as_json(&body),
        json!({
            "error": "sync_requires_plan",
            "message": "Sync needs a Cook Basic or Pro plan. Accounts from before the paywall sync free.",
            "upgrade_url": "https://cook.md/pricing",
        })
    );

    std::env::remove_var("SYNC_ENFORCEMENT");
}

#[tokio::test]
async fn missing_entitlement_under_off_is_not_blocked_by_the_guard() {
    let _guard = env_lock().await;
    std::env::set_var("JWT_SECRET", JWT_SECRET);
    std::env::remove_var("SYNC_ENFORCEMENT"); // default is "off"

    // Same claim shape as the enforce case above: valid token, no
    // `sync_until` at all.
    let token = sign(&json!({ "uid": 1, "exp": far_future_exp() }));

    let (status, _body) = get_chunk(Some(&token)).await;

    // "off" never rejects on entitlement grounds, so neither auth-related
    // status should appear here. The extractor let the request through to
    // `chunks::retrieve`, which then looks for a chunk file that genuinely
    // doesn't exist on disk and 404s -- proving the extractor's decision
    // without needing any DB pool.
    assert_ne!(status, StatusCode::UNAUTHORIZED);
    assert_ne!(status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(status, StatusCode::NOT_FOUND);
}
