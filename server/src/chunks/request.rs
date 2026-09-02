use std::convert::Infallible;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

pub struct RawContentType(pub String);

impl<S> FromRequestParts<S> for RawContentType
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let header = parts
            .headers
            .get("Content-Type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");

        Ok(RawContentType(header.to_string()))
    }
}
