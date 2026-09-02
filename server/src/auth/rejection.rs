//! JSON error bodies for the statuses the auth extractors return.
//!
//! `EntitledUser` (see `auth::entitlement`) rejects with 402 when
//! `SYNC_ENFORCEMENT=enforce` and the account's sync entitlement is
//! missing/expired; `User` and `EntitledUser` both reject with 401 when the
//! bearer token itself is missing/invalid/expired.
//!
//! Under Rocket these bodies came from registered 401/402 catchers. In Axum
//! an extractor's rejection *is* the response, so the bodies live here and
//! are returned directly by the extractors -- there is no separate
//! registration step a consumer could forget, and no duplicate-registration
//! hazard.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

fn sync_upgrade_url() -> String {
    std::env::var("SYNC_UPGRADE_URL").unwrap_or_else(|_| "https://cook.md/pricing".to_string())
}

pub enum AuthRejection {
    Unauthorized,
    PaymentRequired,
}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        match self {
            AuthRejection::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "unauthorized" })),
            )
                .into_response(),
            AuthRejection::PaymentRequired => (
                StatusCode::PAYMENT_REQUIRED,
                Json(json!({
                    "error": "sync_requires_plan",
                    "message": "Sync needs a Cook Basic or Pro plan. Accounts from before the paywall sync free.",
                    "upgrade_url": sync_upgrade_url(),
                })),
            )
                .into_response(),
        }
    }
}
