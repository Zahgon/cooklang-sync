use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use serde::de::DeserializeOwned;

use crate::error::UnprocessableEntity;

/// Query string extractor that rejects with 422, matching the status Rocket
/// returned when a route's `?<param>` guard could not be satisfied. Axum's
/// own `Query` rejects with 400, which would change the contract clients see.
pub(crate) struct Query<T>(pub(crate) T);

impl<T, S> FromRequestParts<S> for Query<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = UnprocessableEntity;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        serde_urlencoded::from_str(parts.uri.query().unwrap_or_default())
            .map(Query)
            .map_err(|_| UnprocessableEntity)
    }
}
