use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::application::{
    CreateAttachmentCommand, MarkReadCommand, SendBroadcastCommand, SendMessageCommand, Service,
};
use crate::error::ServiceError;

pub fn router(service: Arc<Service>) -> Router {
    Router::new()
        .route("/healthz", get(handle_health))
        .route("/v1/messages", post(handle_send_message))
        .route("/v1/messages/{*target}", get(handle_get_message))
        .route(
            "/v1/mailboxes/{actor_id}/{direction}",
            get(handle_list_mailbox),
        )
        .route("/v1/threads/{thread_id}", get(handle_list_thread))
        .route("/v1/broadcasts", post(handle_send_broadcast))
        .route("/v1/broadcasts/{*target}", get(handle_get_broadcast))
        .route("/v1/states/read", post(handle_mark_read))
        .route("/v1/attachments", post(handle_create_attachment))
        .route("/v1/attachments/{*target}", get(handle_get_attachment))
        .with_state(service)
}

async fn handle_health() -> Response {
    json_response(StatusCode::OK, serde_json::json!({ "status": "ok" }))
}

async fn handle_send_message(State(service): State<Arc<Service>>, body: Bytes) -> Response {
    let command = match decode_json::<SendMessageCommand>(&body) {
        Ok(command) => command,
        Err(error) => return json_response(StatusCode::BAD_REQUEST, error),
    };
    match service.send_message(command).await {
        Ok(result) => json_response(StatusCode::CREATED, result),
        Err(error) => error_response(error),
    }
}

async fn handle_get_message(
    State(service): State<Arc<Service>>,
    Path(target): Path<String>,
) -> Response {
    match service.get_message(target.trim_start_matches('/')).await {
        Ok(message) => json_response(StatusCode::OK, message),
        Err(error) => error_response(error),
    }
}

async fn handle_list_mailbox(
    State(service): State<Arc<Service>>,
    Path((actor_id, direction)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let limit = parse_limit(&query);
    let cursor = query.get("cursor").map(String::as_str).unwrap_or("");
    match service
        .list_mailbox(&actor_id, &direction, cursor, limit)
        .await
    {
        Ok(result) => json_response(StatusCode::OK, result),
        Err(error) => error_response(error),
    }
}

async fn handle_list_thread(
    State(service): State<Arc<Service>>,
    Path(thread_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let limit = parse_limit(&query);
    let cursor = query.get("cursor").map(String::as_str).unwrap_or("");
    let actor_id = query.get("actorId").map(String::as_str).unwrap_or("");
    match service
        .list_thread(&thread_id, actor_id, cursor, limit)
        .await
    {
        Ok(result) => json_response(StatusCode::OK, result),
        Err(error) => error_response(error),
    }
}

async fn handle_send_broadcast(State(service): State<Arc<Service>>, body: Bytes) -> Response {
    let command = match decode_json::<SendBroadcastCommand>(&body) {
        Ok(command) => command,
        Err(error) => return json_response(StatusCode::BAD_REQUEST, error),
    };
    match service.send_broadcast(command).await {
        Ok(result) => json_response(StatusCode::CREATED, result),
        Err(error) => error_response(error),
    }
}

async fn handle_get_broadcast(
    State(service): State<Arc<Service>>,
    Path(target): Path<String>,
) -> Response {
    match service.get_broadcast(target.trim_start_matches('/')).await {
        Ok(broadcast) => json_response(StatusCode::OK, broadcast),
        Err(error) => error_response(error),
    }
}

async fn handle_mark_read(State(service): State<Arc<Service>>, body: Bytes) -> Response {
    let command = match decode_json::<MarkReadCommand>(&body) {
        Ok(command) => command,
        Err(error) => return json_response(StatusCode::BAD_REQUEST, error),
    };
    match service.mark_read(command).await {
        Ok(result) => json_response(StatusCode::CREATED, result),
        Err(error) => error_response(error),
    }
}

async fn handle_create_attachment(State(service): State<Arc<Service>>, body: Bytes) -> Response {
    let command = match decode_json::<CreateAttachmentCommand>(&body) {
        Ok(command) => command,
        Err(error) => return json_response(StatusCode::BAD_REQUEST, error),
    };
    match service.create_attachment(command).await {
        Ok(result) => json_response(StatusCode::CREATED, result),
        Err(error) => error_response(error),
    }
}

async fn handle_get_attachment(
    State(service): State<Arc<Service>>,
    Path(target): Path<String>,
) -> Response {
    match service.get_attachment(target.trim_start_matches('/')).await {
        Ok(metadata) => json_response(StatusCode::OK, metadata),
        Err(error) => error_response(error),
    }
}

fn decode_json<T: DeserializeOwned>(body: &[u8]) -> std::result::Result<T, ErrorBody> {
    serde_json::from_slice(body).map_err(|error| error_body("bad_request", error))
}

fn parse_limit(query: &HashMap<String, String>) -> usize {
    query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_default()
}

fn error_response(error: ServiceError) -> Response {
    match error {
        ServiceError::Validation(message) => json_response(
            StatusCode::BAD_REQUEST,
            ErrorBody {
                error: "validation_error".to_string(),
                message: format!("validation error: {message}"),
            },
        ),
        ServiceError::InvalidCursor => json_response(
            StatusCode::BAD_REQUEST,
            ErrorBody {
                error: "validation_error".to_string(),
                message: "invalid cursor".to_string(),
            },
        ),
        ServiceError::ObjectNotFound => json_response(
            StatusCode::NOT_FOUND,
            ErrorBody {
                error: "not_found".to_string(),
                message: "object not found".to_string(),
            },
        ),
        ServiceError::ObjectAlreadyExists => json_response(
            StatusCode::CONFLICT,
            ErrorBody {
                error: "already_exists".to_string(),
                message: "object already exists".to_string(),
            },
        ),
        ServiceError::InvalidObjectKey => json_response(
            StatusCode::BAD_REQUEST,
            ErrorBody {
                error: "validation_error".to_string(),
                message: "invalid object key".to_string(),
            },
        ),
        other => {
            eprintln!("internal service error: {other:?}");
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorBody {
                    error: "internal_error".to_string(),
                    message: "internal server error".to_string(),
                },
            )
        }
    }
}

fn error_body(error: &str, message: impl ToString) -> ErrorBody {
    ErrorBody {
        error: error.to_string(),
        message: message.to_string(),
    }
}

fn json_response<T: Serialize>(status: StatusCode, value: T) -> Response {
    (status, Json(value)).into_response()
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
    message: String,
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Method, Request};
    use chrono::{DateTime, TimeZone, Utc};
    use serde_json::json;
    use tower::ServiceExt;

    use super::*;
    use crate::application::{SendMessageResult, ServiceOptions};
    use crate::ids::IdGenerator;
    use crate::keys::KeyBuilder;
    use crate::storage::ObjectStore;
    use crate::storage::localfs::LocalFileStore;

    #[tokio::test]
    async fn http_server_send_and_get_message() {
        let fixed_now = Utc.with_ymd_and_hms(2026, 6, 1, 11, 22, 33).unwrap();
        let (_temp_dir, service) = new_test_service(fixed_now);
        let app = router(service);

        let body = json!({
            "senderActorId": "actor-a",
            "recipientActorIds": ["actor-b"],
            "messageType": "text",
            "payload": { "text": "hello" }
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let send_result: SendMessageResult = serde_json::from_slice(&bytes).expect("json");
        assert!(!send_result.message_id.is_empty());

        let get_response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/v1/messages/{}", send_result.message_id))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(get_response.status(), StatusCode::OK);
    }

    fn new_test_service(now: DateTime<Utc>) -> (tempfile::TempDir, Arc<Service>) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileStore::new(temp_dir.path()).expect("store"));
        let service = Arc::new(Service::new(ServiceOptions {
            store,
            key_builder: KeyBuilder::new(""),
            id_generator: IdGenerator::new(),
            clock: Some(Arc::new(move || now)),
            max_page_size: 50,
            read_lookback_minutes: 120,
        }));
        (temp_dir, service)
    }
}
