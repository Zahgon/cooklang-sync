use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use super::rejection::AuthRejection;
use super::token::{decode_token, Claims};
use super::user::User;

fn secret() -> String {
    std::env::var("JWT_SECRET").expect("JWT_SECRET must be set.")
}

/// Pulls the `Authorization: Bearer <jwt>` header off `parts` and decodes
/// it into `Claims`. Shared by both the plain `User` extractor and the
/// entitlement-aware `EntitledUser` extractor so they agree on what counts as
/// "authenticated" and so `EntitledUser` can see claims (like `sync_until`)
/// that `User` itself doesn't need.
pub(super) fn extract_claims(parts: &Parts) -> Result<Claims, ()> {
    let auth_header = parts
        .headers
        .get("Authorization")
        .and_then(|value| value.to_str().ok())
        .ok_or(())?;
    let token = auth_header.strip_prefix("Bearer ").ok_or(())?;
    decode_token(token, secret().as_bytes())
}

impl<S> FromRequestParts<S> for User
where
    S: Send + Sync,
{
    type Rejection = AuthRejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match extract_claims(parts) {
            Ok(claim) => Ok(User { id: claim.uid }),
            Err(_) => Err(AuthRejection::Unauthorized),
        }
    }
}
