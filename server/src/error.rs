use axum::body::Body;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

/// Handler error for a failed database query. Replaces Rocket's
/// `response::Debug<diesel::result::Error>`, which likewise logged the error
/// and rendered a bare 500.
pub(crate) struct AppError(diesel::result::Error);

impl From<diesel::result::Error> for AppError {
    fn from(error: diesel::result::Error) -> Self {
        AppError(error)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::error!("database error: {}", self.0);
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}

/// Rejection for a query string that is missing or has the wrong shape.
/// Rocket answered 422 for both, so the port keeps that status rather than
/// Axum's own 400 for a failed `Query` extraction.
pub(crate) struct UnprocessableEntity;

impl IntoResponse for UnprocessableEntity {
    fn into_response(self) -> Response {
        StatusCode::UNPROCESSABLE_ENTITY.into_response()
    }
}

/// `(status, reason, description)` for every status the server can answer with
/// a framework-generated error. The three strings are transcribed from the
/// responses the Rocket build actually sent, not from a specification, because
/// [`error_page`] reproduces those responses byte for byte.
const ERROR_PAGES: &[(u16, &str, &str)] = &[
    (
        404,
        "Not Found",
        "The requested resource could not be found.",
    ),
    (
        413,
        "Payload Too Large",
        "The request is larger than the server is willing or able to process.",
    ),
    (
        415,
        "Unsupported Media Type",
        "The request entity has a media type which the server or resource does not support.",
    ),
    (
        422,
        "Unprocessable Entity",
        "The request was well-formed but was unable to be followed due to semantic errors.",
    ),
    (
        500,
        "Internal Server Error",
        "The server encountered an internal error while processing this request.",
    ),
];

/// Renders the default error page for `status`, or `None` for a status the
/// server never produced under Rocket.
///
/// Rocket answered every framework-generated error with this HTML page rather
/// than an empty body, and the page is reproduced here byte for byte — trailing
/// newline included, which is to say absent. The `<small>Rocket</small>` footer
/// is part of those bytes and is kept for that reason alone; nothing in the
/// crate depends on Rocket any more.
fn error_page(status: StatusCode) -> Option<String> {
    let (code, reason, description) = ERROR_PAGES
        .iter()
        .find(|(code, _, _)| *code == status.as_u16())?;

    Some(format!(
        "<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\">
    <meta name=\"color-scheme\" content=\"light dark\">
    <title>{code} {reason}</title>
</head>
<body align=\"center\">
    <div role=\"main\" align=\"center\">
        <h1>{code}: {reason}</h1>
        <p>{description}</p>
        <hr />
    </div>
    <div role=\"contentinfo\" align=\"center\">
        <small>Rocket</small>
    </div>
</body>
</html>"
    ))
}

/// Response middleware that gives framework-generated errors the body Rocket
/// gave them. Registered as the outermost layer of both routers, so it also
/// covers the 500 raised by the panic catcher and the 413 raised by the body
/// limit.
///
/// A JSON body is left alone: 401 and 402 are rendered by the auth extractors
/// (see [`crate::auth::rejection`]) and are the application's own output, not
/// the framework's. Under Rocket those two came from registered catchers,
/// which took precedence over the default pages in exactly the same way.
pub(crate) async fn default_error_pages(response: Response) -> Response {
    let status = response.status();

    if !status.is_client_error() && !status.is_server_error() {
        return response;
    }

    let is_json = response
        .headers()
        .get(CONTENT_TYPE)
        .map(|value| value.as_bytes().starts_with(b"application/json"))
        .unwrap_or(false);

    if is_json {
        return response;
    }

    let Some(page) = error_page(status) else {
        return response;
    };

    let mut rendered = Response::new(Body::from(page));
    *rendered.status_mut() = status;
    rendered.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );

    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Byte lengths of the pages the Rocket build served, measured from its
    /// responses. A template edit that changed a single character would move
    /// these numbers.
    #[test]
    fn every_page_is_the_length_rocket_served() {
        for (status, length) in [
            (StatusCode::NOT_FOUND, 435),
            (StatusCode::PAYLOAD_TOO_LARGE, 477),
            (StatusCode::UNSUPPORTED_MEDIA_TYPE, 501),
            (StatusCode::UNPROCESSABLE_ENTITY, 496),
            (StatusCode::INTERNAL_SERVER_ERROR, 488),
        ] {
            assert_eq!(error_page(status).expect("page").len(), length, "{status}");
        }
    }

    #[test]
    fn the_404_page_is_reproduced_verbatim() {
        assert_eq!(
            error_page(StatusCode::NOT_FOUND).expect("page"),
            "<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\">
    <meta name=\"color-scheme\" content=\"light dark\">
    <title>404 Not Found</title>
</head>
<body align=\"center\">
    <div role=\"main\" align=\"center\">
        <h1>404: Not Found</h1>
        <p>The requested resource could not be found.</p>
        <hr />
    </div>
    <div role=\"contentinfo\" align=\"center\">
        <small>Rocket</small>
    </div>
</body>
</html>"
        );
    }

    #[test]
    fn a_status_rocket_never_produced_has_no_page() {
        assert!(error_page(StatusCode::IM_A_TEAPOT).is_none());
        assert!(error_page(StatusCode::BAD_REQUEST).is_none());
    }

    #[tokio::test]
    async fn a_json_error_body_is_left_untouched() {
        let mut response = Response::new(Body::from(r#"{"error":"unauthorized"}"#));
        *response.status_mut() = StatusCode::UNAUTHORIZED;
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );

        let response = default_error_pages(response).await;

        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn a_success_response_is_left_untouched() {
        let mut response = Response::new(Body::from("chunk-content"));
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );

        let response = default_error_pages(response).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn an_empty_framework_error_gains_the_rocket_page() {
        let response =
            default_error_pages(StatusCode::NOT_FOUND.into_response()).await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
    }
}
