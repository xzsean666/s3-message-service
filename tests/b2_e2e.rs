use std::sync::Arc;

use chrono::{Duration, Utc};
use s3_message_service::application::{
    CreateAttachmentCommand, MarkReadCommand, SendBroadcastCommand, SendMessageCommand, Service,
    ServiceOptions,
};
use s3_message_service::error::{Result, ServiceError};
use s3_message_service::ids::IdGenerator;
use s3_message_service::keys::KeyBuilder;
use s3_message_service::storage::b2::{B2Config, B2ObjectStore};
use s3_message_service::storage::{ListInput, ObjectStore, PutOptions};
use serde_json::json;

#[tokio::test]
#[ignore = "requires a real Backblaze B2 bucket configured in .env.test"]
async fn b2_real_environment_covers_storage_and_service_features() {
    let Some(config) = load_b2_config_or_skip() else {
        return;
    };
    let store = Arc::new(
        B2ObjectStore::from_config(config)
            .await
            .expect("create B2 object store"),
    );
    let namespace = test_namespace();
    let service_store: Arc<dyn ObjectStore> = store.clone();
    let service = Service::new(ServiceOptions {
        store: service_store,
        key_builder: KeyBuilder::new(&namespace),
        id_generator: IdGenerator::new(),
        clock: Some(Arc::new({
            let fixed_now = Utc::now();
            move || fixed_now
        })),
        max_page_size: 50,
        read_lookback_minutes: 2,
    });

    let result = run_e2e(&service, store.as_ref(), &namespace).await;
    if let Err(error) = cleanup_prefix(store.as_ref(), &format!("{namespace}/")).await {
        eprintln!("B2 e2e cleanup failed for prefix {namespace}/: {error}");
    }
    result.expect("B2 e2e scenario");
}

async fn run_e2e(service: &Service, store: &dyn ObjectStore, namespace: &str) -> Result<()> {
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

    eprintln!("B2 e2e: message read state");
    let read_result = service
        .mark_read(MarkReadCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "read-message-1".to_string(),
            actor_id: "actor-b".to_string(),
            target_kind: "messages".to_string(),
            target_id: send_result.message_id.clone(),
            read_at: Some(Utc::now() + Duration::seconds(1)),
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
    assert!(item_with_state.read_state.is_some());

    eprintln!("B2 e2e: thread listing and thread read state");
    let thread_before_state = service
        .list_thread(&send_result.thread_id, "actor-b", "", 1)
        .await?;
    assert_eq!(thread_before_state.items.len(), 1);
    assert_eq!(
        thread_before_state.items[0].message.id,
        send_result.message_id
    );

    let thread_read_result = service
        .mark_read(MarkReadCommand {
            caller_id: "b2-e2e".to_string(),
            idempotency_key: "read-thread-1".to_string(),
            actor_id: "actor-b".to_string(),
            target_kind: "threads".to_string(),
            target_id: send_result.thread_id.clone(),
            read_position: send_result.message_id.clone(),
            read_at: Some(Utc::now() + Duration::seconds(2)),
            ..Default::default()
        })
        .await?;
    assert!(!thread_read_result.state_event_object_key.is_empty());
    let thread_after_state = service
        .list_thread(&send_result.thread_id, "actor-b", "", 1)
        .await?;
    assert!(thread_after_state.read_state.is_some());

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
    let tag_broadcast_by_id = service.get_broadcast(&tag_broadcast.broadcast_id).await?;
    assert_eq!(tag_broadcast_by_id.audience_type, "tag");
    assert_eq!(tag_broadcast_by_id.audience_keys, vec!["beta"]);
    let tag_broadcast_by_key = service
        .get_broadcast(&tag_broadcast.broadcast_object_key)
        .await?;
    assert_eq!(tag_broadcast_by_key.id, tag_broadcast.broadcast_id);

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
    let all_broadcast_by_id = service.get_broadcast(&all_broadcast.broadcast_id).await?;
    assert_eq!(all_broadcast_by_id.audience_keys, vec!["all"]);

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
    let explicit_broadcast_by_id = service
        .get_broadcast(&explicit_broadcast.broadcast_id)
        .await?;
    assert_eq!(explicit_broadcast_by_id.audience_type, "explicit");
    assert_eq!(explicit_broadcast_by_id.audience_keys.len(), 2);

    eprintln!("B2 e2e: scenario complete");
    Ok(())
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
            let _ = store.delete(&object.key).await;
        }
    }
}

fn load_b2_config_or_skip() -> Option<B2Config> {
    let _ = dotenvy::from_filename(".env.test");
    match B2Config::from_env() {
        Ok(config) => Some(config),
        Err(error) => {
            eprintln!("skipping B2 e2e test: {error}");
            None
        }
    }
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
