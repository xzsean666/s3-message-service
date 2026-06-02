use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use chrono::{DateTime, Duration, Utc};
use futures::FutureExt;
use s3_message_service::application::{
    CreateAttachmentCommand, ListMailboxResult, MarkReadCommand, SendBroadcastCommand,
    SendMessageCommand, SendMessageResult, Service, ServiceOptions,
};
use s3_message_service::error::{Result, ServiceError};
use s3_message_service::httpapi;
use s3_message_service::ids::IdGenerator;
use s3_message_service::keys::KeyBuilder;
use s3_message_service::storage::b2::{B2Config, B2ObjectStore};
use s3_message_service::storage::{ListInput, ObjectStore, PutOptions};
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
#[ignore = "requires a real Backblaze B2 bucket configured in .env.test"]
async fn b2_real_environment_covers_storage_and_service_features() {
    let config = load_b2_config();
    let store = Arc::new(
        B2ObjectStore::from_config(config)
            .await
            .expect("create B2 object store"),
    );
    let namespace = test_namespace();
    let test_clock = TestClock::new(Utc::now());
    let service_store: Arc<dyn ObjectStore> = store.clone();
    let service = Arc::new(Service::new(ServiceOptions {
        store: service_store,
        key_builder: KeyBuilder::new(&namespace),
        id_generator: IdGenerator::new(),
        clock: Some(test_clock.clock()),
        max_page_size: 50,
        read_lookback_minutes: 2,
    }));

    let result = AssertUnwindSafe(run_e2e(
        service.clone(),
        store.as_ref(),
        &namespace,
        &test_clock,
    ))
    .catch_unwind()
    .await;
    cleanup_prefix(store.as_ref(), &format!("{namespace}/"))
        .await
        .expect("B2 e2e cleanup");
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("B2 e2e scenario failed: {error:?}"),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

async fn run_e2e(
    service: Arc<Service>,
    store: &dyn ObjectStore,
    namespace: &str,
    test_clock: &TestClock,
) -> Result<()> {
    eprintln!("B2 e2e: storage contract");
    storage_contract(store, namespace).await?;

    eprintln!("B2 e2e: attachment metadata");
    let attachment = service
        .create_attachment(CreateAttachmentCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "attachment-1".to_string(),
            original_file_name: "Report Final.pdf".to_string(),
            content_type: "application/pdf".to_string(),
            size: 1234,
            checksum: "sha256:e2e".to_string(),
            ..Default::default()
        })
        .await?;
    let attachment_by_id = service.get_attachment(&attachment.attachment_id).await?;
    assert_eq!(attachment_by_id.normalized_file_name, "report-final.pdf");
    let attachment_by_key = service
        .get_attachment(&attachment.attachment_metadata_key)
        .await?;
    assert_eq!(attachment_by_key.id, attachment.attachment_id);

    eprintln!("B2 e2e: send threaded multi-recipient message");
    let send_result = service
        .send_message(SendMessageCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "message-1".to_string(),
            sender_actor_id: "actor-a".to_string(),
            recipient_actor_ids: vec!["actor-b".to_string(), "actor-c".to_string()],
            message_type: "text".to_string(),
            payload: Some(json!({"text": "hello from B2 e2e"})),
            attachment_ids: vec![attachment.attachment_id.clone()],
            create_thread: true,
            ..Default::default()
        })
        .await?;
    assert!(!send_result.message_id.is_empty());
    assert!(!send_result.thread_id.is_empty());
    assert!(send_result.reference_keys.len() >= 4);

    eprintln!("B2 e2e: idempotent retry");
    let retry_result = service
        .send_message(SendMessageCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "message-1".to_string(),
            sender_actor_id: "actor-a".to_string(),
            recipient_actor_ids: vec!["actor-b".to_string(), "actor-c".to_string()],
            message_type: "text".to_string(),
            payload: Some(json!({"text": "hello from B2 e2e"})),
            attachment_ids: vec![attachment.attachment_id.clone()],
            create_thread: true,
            ..Default::default()
        })
        .await?;
    assert_eq!(retry_result.message_id, send_result.message_id);

    let idempotency_conflict = service
        .send_message(SendMessageCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "message-1".to_string(),
            sender_actor_id: "actor-a".to_string(),
            recipient_actor_ids: vec!["actor-b".to_string(), "actor-c".to_string()],
            message_type: "text".to_string(),
            payload: Some(json!({"text": "different payload"})),
            attachment_ids: vec![attachment.attachment_id.clone()],
            create_thread: true,
            ..Default::default()
        })
        .await
        .expect_err("changed request with same idempotency key should conflict");
    assert!(matches!(
        idempotency_conflict,
        ServiceError::IdempotencyConflict
    ));

    let message_by_id = service.get_message(&send_result.message_id).await?;
    assert_eq!(message_by_id.sender_actor_id, "actor-a");
    assert_eq!(message_by_id.attachment_ids, vec![attachment.attachment_id]);
    let message_by_key = service.get_message(&send_result.message_object_key).await?;
    assert_eq!(message_by_key.id, send_result.message_id);

    eprintln!("B2 e2e: send pagination messages");
    for index in 0..2 {
        service
            .send_message(SendMessageCommand {
                caller_id: "b2-e2e".to_string(),
                idempotency_key: format!("page-message-{index}"),
                sender_actor_id: "actor-a".to_string(),
                recipient_actor_ids: vec!["actor-b".to_string()],
                message_type: "text".to_string(),
                payload: Some(json!({"text": format!("page {index}")})),
                ..Default::default()
            })
            .await?;
    }

    eprintln!("B2 e2e: mailbox pagination");
    let sent_mailbox = service.list_mailbox("actor-a", "sent", "", 3).await?;
    assert_eq!(sent_mailbox.items.len(), 3);
    let first_inbox_page = service.list_mailbox("actor-b", "inbox", "", 2).await?;
    assert_eq!(first_inbox_page.items.len(), 2);
    assert!(!first_inbox_page.next_cursor.is_empty());
    let second_inbox_page = service
        .list_mailbox("actor-b", "inbox", &first_inbox_page.next_cursor, 1)
        .await?;
    assert_eq!(second_inbox_page.items.len(), 1);

    eprintln!("B2 e2e: actor key collision resistance");
    let slash_actor = "edge/a";
    let space_actor = "edge a";
    service
        .send_message(SendMessageCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "collision-slash".to_string(),
            sender_actor_id: "actor-a".to_string(),
            recipient_actor_ids: vec![slash_actor.to_string()],
            message_type: "text".to_string(),
            payload: Some(json!({"text": "slash actor only"})),
            ..Default::default()
        })
        .await?;
    service
        .send_message(SendMessageCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "collision-space".to_string(),
            sender_actor_id: "actor-a".to_string(),
            recipient_actor_ids: vec![space_actor.to_string()],
            message_type: "text".to_string(),
            payload: Some(json!({"text": "space actor only"})),
            ..Default::default()
        })
        .await?;
    assert_mailbox_texts(service.as_ref(), slash_actor, &["slash actor only"]).await?;
    assert_mailbox_texts(service.as_ref(), space_actor, &["space actor only"]).await?;

    eprintln!("B2 e2e: message read state");
    let message_read_at = Utc::now() + Duration::seconds(1);
    let read_result = service
        .mark_read(MarkReadCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "read-message-1".to_string(),
            actor_id: "actor-b".to_string(),
            target_kind: "messages".to_string(),
            target_id: send_result.message_id.clone(),
            read_at: Some(message_read_at),
            ..Default::default()
        })
        .await?;
    assert!(!read_result.current_state_object_key.is_empty());

    let inbox_with_state = service.list_mailbox("actor-b", "inbox", "", 3).await?;
    let item_with_state = inbox_with_state
        .items
        .iter()
        .find(|item| item.message.id == send_result.message_id)
        .expect("message should be in actor-b inbox");
    let read_state = item_with_state
        .read_state
        .as_ref()
        .expect("message read state");
    assert_eq!(read_state.actor_id, "actor-b");
    assert_eq!(read_state.target_kind, "messages");
    assert_eq!(read_state.target_id, send_result.message_id);
    assert_eq!(read_state.read_at, message_read_at);

    service
        .mark_read(MarkReadCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "read-message-older".to_string(),
            actor_id: "actor-b".to_string(),
            target_kind: "messages".to_string(),
            target_id: send_result.message_id.clone(),
            read_at: Some(message_read_at - Duration::seconds(30)),
            ..Default::default()
        })
        .await?;
    let inbox_after_older_state = service.list_mailbox("actor-b", "inbox", "", 3).await?;
    let state_after_older = inbox_after_older_state
        .items
        .iter()
        .find(|item| item.message.id == send_result.message_id)
        .and_then(|item| item.read_state.as_ref())
        .expect("message read state after older update");
    assert_eq!(state_after_older.read_at, message_read_at);

    eprintln!("B2 e2e: thread listing and thread read state");
    let thread_before_state = service
        .list_thread(&send_result.thread_id, "actor-b", "", 1)
        .await?;
    assert_eq!(thread_before_state.items.len(), 1);
    assert_eq!(
        thread_before_state.items[0].message.id,
        send_result.message_id
    );

    let thread_read_at = Utc::now() + Duration::seconds(2);
    let thread_read_result = service
        .mark_read(MarkReadCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "read-thread-1".to_string(),
            actor_id: "actor-b".to_string(),
            target_kind: "threads".to_string(),
            target_id: send_result.thread_id.clone(),
            read_position: send_result.message_id.clone(),
            read_at: Some(thread_read_at),
        })
        .await?;
    assert!(!thread_read_result.state_event_object_key.is_empty());
    let thread_after_state = service
        .list_thread(&send_result.thread_id, "actor-b", "", 1)
        .await?;
    let thread_state = thread_after_state
        .read_state
        .as_ref()
        .expect("thread read state");
    assert_eq!(thread_state.actor_id, "actor-b");
    assert_eq!(thread_state.target_kind, "threads");
    assert_eq!(thread_state.target_id, send_result.thread_id);
    assert_eq!(thread_state.read_position, send_result.message_id);
    assert_eq!(thread_state.read_at, thread_read_at);

    eprintln!("B2 e2e: threaded reply inheritance");
    let reply_result = service
        .send_message(SendMessageCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "reply-1".to_string(),
            sender_actor_id: "actor-b".to_string(),
            recipient_actor_ids: vec!["actor-a".to_string()],
            message_type: "text".to_string(),
            payload: Some(json!({"text": "reply from B2 e2e"})),
            parent_message_id: send_result.message_id.clone(),
            ..Default::default()
        })
        .await?;
    assert_eq!(reply_result.thread_id, send_result.thread_id);
    let reply_message = service.get_message(&reply_result.message_id).await?;
    assert_eq!(reply_message.thread_id, send_result.thread_id);
    assert_eq!(reply_message.parent_message_id, send_result.message_id);
    let thread_after_reply = service
        .list_thread(&send_result.thread_id, "actor-a", "", 10)
        .await?;
    assert_eq!(thread_after_reply.items.len(), 2);

    eprintln!("B2 e2e: broadcasts");
    let tag_broadcast = service
        .send_broadcast(SendBroadcastCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "broadcast-tag-1".to_string(),
            sender_actor_id: "system".to_string(),
            audience_type: "tag".to_string(),
            audience_keys: vec!["beta".to_string()],
            message_type: "text".to_string(),
            payload: Some(json!({"text": "tag announcement"})),
            ..Default::default()
        })
        .await?;
    assert_eq!(tag_broadcast.audience_object_keys.len(), 1);
    let tag_broadcast_by_id = service.get_broadcast(&tag_broadcast.broadcast_id).await?;
    assert_eq!(tag_broadcast_by_id.audience_type, "tag");
    assert_eq!(tag_broadcast_by_id.audience_keys, vec!["beta"]);
    let tag_broadcast_by_key = service
        .get_broadcast(&tag_broadcast.broadcast_object_key)
        .await?;
    assert_eq!(tag_broadcast_by_key.id, tag_broadcast.broadcast_id);
    assert_lookup_points_to(
        store,
        &tag_broadcast.audience_object_keys[0],
        &tag_broadcast.broadcast_object_key,
    )
    .await?;

    let all_broadcast = service
        .send_broadcast(SendBroadcastCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "broadcast-all-1".to_string(),
            sender_actor_id: "system".to_string(),
            audience_type: "all".to_string(),
            message_type: "text".to_string(),
            payload: Some(json!({"text": "all announcement"})),
            ..Default::default()
        })
        .await?;
    assert_eq!(all_broadcast.audience_object_keys.len(), 1);
    let all_broadcast_by_id = service.get_broadcast(&all_broadcast.broadcast_id).await?;
    assert_eq!(all_broadcast_by_id.audience_keys, vec!["all"]);
    assert_lookup_points_to(
        store,
        &all_broadcast.audience_object_keys[0],
        &all_broadcast.broadcast_object_key,
    )
    .await?;

    let explicit_broadcast = service
        .send_broadcast(SendBroadcastCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "broadcast-explicit-1".to_string(),
            sender_actor_id: "system".to_string(),
            audience_type: "explicit".to_string(),
            audience_keys: vec!["actor-b".to_string(), "actor-c".to_string()],
            message_type: "text".to_string(),
            payload: Some(json!({"text": "explicit announcement"})),
            ..Default::default()
        })
        .await?;
    assert_eq!(explicit_broadcast.audience_object_keys.len(), 2);
    let explicit_broadcast_by_id = service
        .get_broadcast(&explicit_broadcast.broadcast_id)
        .await?;
    assert_eq!(explicit_broadcast_by_id.audience_type, "explicit");
    assert_eq!(explicit_broadcast_by_id.audience_keys.len(), 2);
    for audience_object_key in &explicit_broadcast.audience_object_keys {
        assert_lookup_points_to(
            store,
            audience_object_key,
            &explicit_broadcast.broadcast_object_key,
        )
        .await?;
    }

    eprintln!("B2 e2e: cross-minute cursor windows");
    cross_minute_cursor_window(service.as_ref(), test_clock).await?;

    eprintln!("B2 e2e: HTTP router over real B2 store");
    http_router_e2e(service).await?;

    eprintln!("B2 e2e: scenario complete");
    Ok(())
}

#[derive(Clone)]
struct TestClock {
    now: Arc<Mutex<DateTime<Utc>>>,
}

impl TestClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    fn clock(&self) -> Arc<dyn Fn() -> DateTime<Utc> + Send + Sync> {
        let now = self.now.clone();
        Arc::new(move || now.lock().expect("test clock lock poisoned").to_owned())
    }

    fn set(&self, now: DateTime<Utc>) {
        *self.now.lock().expect("test clock lock poisoned") = now;
    }
}

async fn assert_mailbox_texts(
    service: &Service,
    actor_id: &str,
    expected_texts: &[&str],
) -> Result<()> {
    let mailbox = service.list_mailbox(actor_id, "inbox", "", 10).await?;
    let texts = mailbox
        .items
        .iter()
        .map(|item| message_text(&item.message.payload).to_string())
        .collect::<Vec<_>>();
    assert_eq!(texts, expected_texts);
    Ok(())
}

async fn assert_lookup_points_to(
    store: &dyn ObjectStore,
    lookup_key: &str,
    expected_object_key: &str,
) -> Result<()> {
    let data = store.get(lookup_key).await?;
    let value: serde_json::Value = serde_json::from_slice(&data)?;
    assert_eq!(value["entityKind"].as_str(), Some("broadcast"));
    assert_eq!(value["objectKey"].as_str(), Some(expected_object_key));
    Ok(())
}

async fn cross_minute_cursor_window(service: &Service, test_clock: &TestClock) -> Result<()> {
    let base = Utc::now() + Duration::minutes(20);
    let recipient = "window-recipient";
    let windows = [
        ("window-newest", base),
        ("window-middle", base - Duration::minutes(1)),
        ("window-oldest", base - Duration::minutes(2)),
    ];

    for (index, (text, instant)) in windows.iter().enumerate() {
        test_clock.set(*instant);
        service
            .send_message(SendMessageCommand {
                caller_id: "b2-e2e".to_string(),
                idempotency_key: format!("window-message-{index}"),
                sender_actor_id: "window-sender".to_string(),
                recipient_actor_ids: vec![recipient.to_string()],
                message_type: "text".to_string(),
                payload: Some(json!({ "text": text })),
                ..Default::default()
            })
            .await?;
    }

    test_clock.set(base);
    let first_page = service.list_mailbox(recipient, "inbox", "", 2).await?;
    assert_eq!(
        mailbox_texts(&first_page),
        vec!["window-newest", "window-middle"]
    );
    assert!(!first_page.next_cursor.is_empty());

    let second_page = service
        .list_mailbox(recipient, "inbox", &first_page.next_cursor, 2)
        .await?;
    assert_eq!(mailbox_texts(&second_page), vec!["window-oldest"]);
    assert!(second_page.next_cursor.is_empty());
    Ok(())
}

async fn http_router_e2e(service: Arc<Service>) -> Result<()> {
    let app = httpapi::router(service);
    let body = json!({
        "callerId": "http-e2e",
        "idempotencyKey": "http-message-1",
        "senderActorId": "http-sender",
        "recipientActorIds": ["http-recipient"],
        "messageType": "text",
        "payload": { "text": "hello over http" }
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("build send request"),
        )
        .await
        .expect("send response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("send response body");
    let send_result: SendMessageResult = serde_json::from_slice(&bytes)?;

    let get_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/v1/messages/{}", send_result.message_id))
                .body(Body::empty())
                .expect("build get request"),
        )
        .await
        .expect("get response");
    assert_eq!(get_response.status(), StatusCode::OK);

    let mailbox_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/v1/mailboxes/http-recipient/inbox?limit=1")
                .body(Body::empty())
                .expect("build mailbox request"),
        )
        .await
        .expect("mailbox response");
    assert_eq!(mailbox_response.status(), StatusCode::OK);
    let bytes = to_bytes(mailbox_response.into_body(), usize::MAX)
        .await
        .expect("mailbox response body");
    let mailbox: ListMailboxResult = serde_json::from_slice(&bytes)?;
    assert_eq!(mailbox.items.len(), 1);
    assert_eq!(mailbox.items[0].message.id, send_result.message_id);

    let conflict_body = json!({
        "callerId": "http-e2e",
        "idempotencyKey": "http-message-1",
        "senderActorId": "http-sender",
        "recipientActorIds": ["http-recipient"],
        "messageType": "text",
        "payload": { "text": "changed over http" }
    });
    let conflict_response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(conflict_body.to_string()))
                .expect("build conflict request"),
        )
        .await
        .expect("conflict response");
    assert_eq!(conflict_response.status(), StatusCode::CONFLICT);
    let bytes = to_bytes(conflict_response.into_body(), usize::MAX)
        .await
        .expect("conflict response body");
    let error: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(error["error"].as_str(), Some("idempotency_conflict"));
    Ok(())
}

fn mailbox_texts(mailbox: &ListMailboxResult) -> Vec<&str> {
    mailbox
        .items
        .iter()
        .map(|item| message_text(&item.message.payload))
        .collect()
}

fn message_text(payload: &serde_json::Value) -> &str {
    payload
        .get("text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
}

async fn storage_contract(store: &dyn ObjectStore, namespace: &str) -> Result<()> {
    let object_key = format!("{namespace}/contract/object.json");
    let prefix = format!("{namespace}/contract/");
    store
        .put(
            &object_key,
            br#"{"ok":true}"#,
            PutOptions {
                create_only: true,
                content_type: "application/json".to_string(),
            },
        )
        .await?;
    let conflict = store
        .put(
            &object_key,
            b"{}",
            PutOptions {
                create_only: true,
                content_type: "application/json".to_string(),
            },
        )
        .await
        .expect_err("create-only put should conflict");
    assert!(matches!(conflict, ServiceError::ObjectAlreadyExists));

    let data = store.get(&object_key).await?;
    assert_eq!(data, br#"{"ok":true}"#);
    let head = store.head(&object_key).await?;
    assert_eq!(head.key, object_key);
    assert_eq!(head.size, br#"{"ok":true}"#.len() as u64);

    let page = store
        .list(ListInput {
            prefix,
            start_after: String::new(),
            limit: 10,
        })
        .await?;
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].key, object_key);

    store.delete(&object_key).await?;
    let missing = store
        .get(&object_key)
        .await
        .expect_err("deleted object should not be readable");
    assert!(matches!(missing, ServiceError::ObjectNotFound));
    Ok(())
}

async fn cleanup_prefix(store: &dyn ObjectStore, prefix: &str) -> Result<()> {
    eprintln!("B2 e2e: cleanup prefix {prefix}");
    loop {
        let page = store
            .list(ListInput {
                prefix: prefix.to_string(),
                start_after: String::new(),
                limit: 1000,
            })
            .await?;
        if page.objects.is_empty() {
            return Ok(());
        }
        for object in page.objects {
            store.delete(&object.key).await?;
        }
    }
}

fn load_b2_config() -> B2Config {
    dotenvy::from_filename(".env.test").expect("load .env.test for real B2 e2e");
    B2Config::from_env().expect("real B2 e2e requires B2 settings in .env.test")
}

fn test_namespace() -> String {
    let base = std::env::var("B2_TEST_PREFIX")
        .ok()
        .map(|value| value.trim().trim_matches('/').to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "e2e".to_string());
    let id = IdGenerator::new()
        .new_id()
        .expect("generate e2e namespace id");
    format!("{base}-{id}")
}
