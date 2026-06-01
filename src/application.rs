use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex as AsyncMutex;

use crate::cursors::{self, Cursor, Direction};
use crate::domain::{
    AttachmentMetadata, Broadcast, LookupReference, Message, MessageReference, OperationRecord,
    OperationStep, SCHEMA_VERSION, State, Thread,
};
use crate::error::{Result, ServiceError};
use crate::ids::IdGenerator;
use crate::keys::{KeyBuilder, normalize_external_id};
use crate::storage::{ListInput, ObjectStore, PutOptions};

const DEFAULT_CALLER_ID: &str = "default";
const HYDRATION_CONCURRENCY: usize = 8;
const IDEMPOTENCY_LOCK_SHARDS: usize = 64;

pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

pub struct Service {
    store: Arc<dyn ObjectStore>,
    key_builder: KeyBuilder,
    id_generator: IdGenerator,
    clock: Clock,
    max_page_size: usize,
    read_lookback_minutes: usize,
    non_atomic_create_lock: Arc<AsyncMutex<()>>,
    idempotency_locks: Vec<Arc<AsyncMutex<()>>>,
}

pub struct ServiceOptions {
    pub store: Arc<dyn ObjectStore>,
    pub key_builder: KeyBuilder,
    pub id_generator: IdGenerator,
    pub clock: Option<Clock>,
    pub max_page_size: usize,
    pub read_lookback_minutes: usize,
}

impl Service {
    pub fn new(options: ServiceOptions) -> Self {
        let max_page_size = if options.max_page_size == 0 {
            100
        } else {
            options.max_page_size
        };
        let read_lookback_minutes = if options.read_lookback_minutes == 0 {
            43_200
        } else {
            options.read_lookback_minutes
        };

        Self {
            store: options.store,
            key_builder: options.key_builder,
            id_generator: options.id_generator,
            clock: options.clock.unwrap_or_else(|| Arc::new(Utc::now)),
            max_page_size,
            read_lookback_minutes,
            non_atomic_create_lock: Arc::new(AsyncMutex::new(())),
            idempotency_locks: (0..IDEMPOTENCY_LOCK_SHARDS)
                .map(|_| Arc::new(AsyncMutex::new(())))
                .collect(),
        }
    }

    pub async fn send_message(&self, command: SendMessageCommand) -> Result<SendMessageResult> {
        validate_send_message(&command)?;
        let caller_id = caller_or_default(&command.caller_id);
        let _idempotency_guard = self
            .lock_idempotency(&caller_id, &command.idempotency_key)
            .await;
        let mut created_at = self.now();
        let entity_ids = self.message_entity_ids(&command)?;
        let (operation, completed) = self
            .begin_operation(&caller_id, &command.idempotency_key, entity_ids, created_at)
            .await?;
        if completed {
            return decode_operation_result(operation.result);
        }

        let entity_ids = operation.entity_ids.clone();
        created_at = operation.created_at;
        let message_id = required_entity_id(&entity_ids, "messageId")?;
        let mut thread_id = command.thread_id.clone();
        if thread_id.is_empty() {
            thread_id = entity_ids.get("threadId").cloned().unwrap_or_default();
        }

        let message_object_key = self.key_builder.message_body(&message_id, created_at);
        let message = Message {
            schema_version: SCHEMA_VERSION,
            id: message_id.clone(),
            sender_actor_id: command.sender_actor_id.clone(),
            recipient_actor_ids: command.recipient_actor_ids.clone(),
            message_type: command.message_type.clone(),
            payload: normalized_payload(command.payload.clone()),
            attachment_ids: command.attachment_ids.clone(),
            thread_id: thread_id.clone(),
            parent_message_id: command.parent_message_id.clone(),
            created_at,
        };
        self.put_json_create_only(&message_object_key, &message)
            .await?;
        self.record_step(&operation.id, "message-body", &message_object_key)
            .await;

        let message_lookup_key = self.key_builder.message_lookup(&message_id);
        let lookup = LookupReference {
            schema_version: SCHEMA_VERSION,
            entity_kind: "message".to_string(),
            entity_id: message_id.clone(),
            object_key: message_object_key.clone(),
            created_at,
        };
        self.put_json_create_only(&message_lookup_key, &lookup)
            .await?;
        self.record_step(&operation.id, "message-lookup", &message_lookup_key)
            .await;

        let mut reference_keys = Vec::with_capacity(command.recipient_actor_ids.len() + 2);
        let sent_reference_id = required_entity_id(&entity_ids, "sentRefId")?;
        let sent_reference_key = self.key_builder.mailbox_reference(
            &command.sender_actor_id,
            "sent",
            created_at,
            &message_id,
            &sent_reference_id,
        );
        let sent_reference = MessageReference {
            schema_version: SCHEMA_VERSION,
            id: sent_reference_id,
            message_id: message_id.clone(),
            message_object_key: message_object_key.clone(),
            owner_id: command.sender_actor_id.clone(),
            reference_kind: "sent".to_string(),
            created_at,
        };
        self.put_json_create_only(&sent_reference_key, &sent_reference)
            .await?;
        self.record_step(&operation.id, "sent-reference", &sent_reference_key)
            .await;
        reference_keys.push(sent_reference_key);

        for (index, recipient_actor_id) in command.recipient_actor_ids.iter().enumerate() {
            let reference_id = required_entity_id(&entity_ids, &format!("inboxRefId:{index}"))?;
            let reference_key = self.key_builder.mailbox_reference(
                recipient_actor_id,
                "inbox",
                created_at,
                &message_id,
                &reference_id,
            );
            let reference = MessageReference {
                schema_version: SCHEMA_VERSION,
                id: reference_id,
                message_id: message_id.clone(),
                message_object_key: message_object_key.clone(),
                owner_id: recipient_actor_id.clone(),
                reference_kind: "inbox".to_string(),
                created_at,
            };
            self.put_json_create_only(&reference_key, &reference)
                .await?;
            self.record_step(
                &operation.id,
                &format!("inbox-reference-{index}"),
                &reference_key,
            )
            .await;
            reference_keys.push(reference_key);
        }

        if !thread_id.is_empty() {
            let thread_metadata_key = self.key_builder.thread_metadata(&thread_id);
            let thread = Thread {
                schema_version: SCHEMA_VERSION,
                id: thread_id.clone(),
                root_message_id: message_id.clone(),
                external_correlation_id: String::new(),
                created_at,
            };
            self.put_json_create_only(&thread_metadata_key, &thread)
                .await?;
            self.record_step(&operation.id, "thread-metadata", &thread_metadata_key)
                .await;

            let thread_reference_id = required_entity_id(&entity_ids, "threadRefId")?;
            let thread_reference_key = self.key_builder.thread_reference(
                &thread_id,
                created_at,
                &message_id,
                &thread_reference_id,
            );
            let thread_reference = MessageReference {
                schema_version: SCHEMA_VERSION,
                id: thread_reference_id,
                message_id: message_id.clone(),
                message_object_key: message_object_key.clone(),
                owner_id: thread_id.clone(),
                reference_kind: "thread".to_string(),
                created_at,
            };
            self.put_json_create_only(&thread_reference_key, &thread_reference)
                .await?;
            self.record_step(&operation.id, "thread-reference", &thread_reference_key)
                .await;
            reference_keys.push(thread_reference_key);
        }

        let result = SendMessageResult {
            operation_id: operation.id.clone(),
            message_id,
            message_object_key,
            message_lookup_key,
            thread_id,
            reference_keys,
        };
        self.complete_operation(&caller_id, &command.idempotency_key, operation, &result)
            .await?;
        Ok(result)
    }

    pub async fn get_message(&self, message_id_or_key: &str) -> Result<Message> {
        let target = message_id_or_key.trim();
        if target.is_empty() {
            return Err(validation_error("message id or key is required"));
        }
        let object_key = if !target.starts_with("messages/") && !target.contains("/messages/") {
            let lookup: LookupReference = self
                .get_json(&self.key_builder.message_lookup(target))
                .await?;
            lookup.object_key
        } else {
            target.to_string()
        };
        self.get_json(&object_key).await
    }

    pub async fn list_mailbox(
        &self,
        actor_id: &str,
        direction: &str,
        raw_cursor: &str,
        limit: usize,
    ) -> Result<ListMailboxResult> {
        if actor_id.trim().is_empty() {
            return Err(validation_error("actor id is required"));
        }
        if direction != "inbox" && direction != "sent" {
            return Err(validation_error("direction must be inbox or sent"));
        }
        let limit = self.normalize_limit(limit);
        let cursor = self.cursor_or_initial(
            raw_cursor,
            &format!("mailbox:{direction}"),
            actor_id,
            Direction::NewestFirst,
            self.now(),
            limit,
        )?;

        let (references, next_cursor) = self
            .scan_references(cursor, limit, |window| {
                self.key_builder.mailbox_prefix(actor_id, direction, window)
            })
            .await?;

        let items = stream::iter(references)
            .map(|reference| async move {
                let message = self.get_message(&reference.message_object_key).await?;
                let read_state = self
                    .get_current_state(actor_id, "messages", &reference.message_id)
                    .await?;
                Ok::<MailboxItem, ServiceError>(MailboxItem {
                    reference,
                    message,
                    read_state,
                })
            })
            .buffered(HYDRATION_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(ListMailboxResult { items, next_cursor })
    }

    pub async fn list_thread(
        &self,
        thread_id: &str,
        actor_id: &str,
        raw_cursor: &str,
        limit: usize,
    ) -> Result<ListThreadResult> {
        if thread_id.trim().is_empty() {
            return Err(validation_error("thread id is required"));
        }
        let thread: Thread = self
            .get_json(&self.key_builder.thread_metadata(thread_id))
            .await?;
        let limit = self.normalize_limit(limit);
        let cursor = self.cursor_or_initial(
            raw_cursor,
            "thread",
            thread_id,
            Direction::OldestFirst,
            thread.created_at,
            limit,
        )?;

        let (references, next_cursor) = self
            .scan_references(cursor, limit, |window| {
                self.key_builder.thread_prefix(thread_id, window)
            })
            .await?;
        let items = stream::iter(references)
            .map(|reference| async move {
                let message = self.get_message(&reference.message_object_key).await?;
                Ok::<ThreadItem, ServiceError>(ThreadItem { reference, message })
            })
            .buffered(HYDRATION_CONCURRENCY)
            .try_collect()
            .await?;
        let read_state = if actor_id.is_empty() {
            None
        } else {
            self.get_current_state(actor_id, "threads", thread_id)
                .await?
        };
        Ok(ListThreadResult {
            thread,
            items,
            next_cursor,
            read_state,
        })
    }

    pub async fn send_broadcast(
        &self,
        command: SendBroadcastCommand,
    ) -> Result<SendBroadcastResult> {
        validate_send_broadcast(&command)?;
        let caller_id = caller_or_default(&command.caller_id);
        let _idempotency_guard = self
            .lock_idempotency(&caller_id, &command.idempotency_key)
            .await;
        let mut created_at = self.now();
        let mut entity_ids = HashMap::new();
        entity_ids.insert("broadcastId".to_string(), self.id_generator.new_id()?);

        let (operation, completed) = self
            .begin_operation(&caller_id, &command.idempotency_key, entity_ids, created_at)
            .await?;
        if completed {
            return decode_operation_result(operation.result);
        }

        let broadcast_id = required_entity_id(&operation.entity_ids, "broadcastId")?;
        created_at = operation.created_at;
        let broadcast_object_key = self.key_builder.broadcast_body(&broadcast_id, created_at);
        let broadcast = Broadcast {
            schema_version: SCHEMA_VERSION,
            id: broadcast_id.clone(),
            sender_actor_id: command.sender_actor_id.clone(),
            audience_type: command.audience_type.clone(),
            audience_keys: normalize_audience_keys(&command.audience_type, &command.audience_keys),
            message_type: command.message_type.clone(),
            payload: normalized_payload(command.payload.clone()),
            attachment_ids: command.attachment_ids.clone(),
            created_at,
            expires_at: command.expires_at,
        };
        self.put_json_create_only(&broadcast_object_key, &broadcast)
            .await?;
        self.record_step(&operation.id, "broadcast-body", &broadcast_object_key)
            .await;

        let broadcast_lookup_key = self.key_builder.broadcast_lookup(&broadcast_id);
        let lookup = LookupReference {
            schema_version: SCHEMA_VERSION,
            entity_kind: "broadcast".to_string(),
            entity_id: broadcast_id.clone(),
            object_key: broadcast_object_key.clone(),
            created_at,
        };
        self.put_json_create_only(&broadcast_lookup_key, &lookup)
            .await?;
        self.record_step(&operation.id, "broadcast-lookup", &broadcast_lookup_key)
            .await;

        let mut audience_object_keys = Vec::with_capacity(broadcast.audience_keys.len());
        for audience_key in &broadcast.audience_keys {
            let audience_object_key = self.key_builder.broadcast_audience(
                &command.audience_type,
                audience_key,
                created_at,
                &broadcast_id,
            );
            self.put_json_create_only(&audience_object_key, &lookup)
                .await?;
            self.record_step(
                &operation.id,
                &format!("broadcast-audience-{audience_key}"),
                &audience_object_key,
            )
            .await;
            audience_object_keys.push(audience_object_key);
        }

        let result = SendBroadcastResult {
            operation_id: operation.id.clone(),
            broadcast_id,
            broadcast_object_key,
            broadcast_lookup_key,
            audience_object_keys,
        };
        self.complete_operation(&caller_id, &command.idempotency_key, operation, &result)
            .await?;
        Ok(result)
    }

    pub async fn get_broadcast(&self, broadcast_id_or_key: &str) -> Result<Broadcast> {
        let target = broadcast_id_or_key.trim();
        if target.is_empty() {
            return Err(validation_error("broadcast id or key is required"));
        }
        let object_key = if !target.starts_with("broadcast/") && !target.contains("/broadcast/") {
            let lookup: LookupReference = self
                .get_json(&self.key_builder.broadcast_lookup(target))
                .await?;
            lookup.object_key
        } else {
            target.to_string()
        };
        self.get_json(&object_key).await
    }

    pub async fn mark_read(&self, command: MarkReadCommand) -> Result<MarkReadResult> {
        validate_mark_read(&command)?;
        let caller_id = caller_or_default(&command.caller_id);
        let _idempotency_guard = self
            .lock_idempotency(&caller_id, &command.idempotency_key)
            .await;
        let mut created_at = self.now();
        let read_at = command.read_at.unwrap_or(created_at).with_timezone(&Utc);
        let mut entity_ids = HashMap::new();
        entity_ids.insert("stateId".to_string(), self.id_generator.new_id()?);

        let (operation, completed) = self
            .begin_operation(&caller_id, &command.idempotency_key, entity_ids, created_at)
            .await?;
        if completed {
            return decode_operation_result(operation.result);
        }

        let state_id = required_entity_id(&operation.entity_ids, "stateId")?;
        created_at = operation.created_at;
        let state = State {
            schema_version: SCHEMA_VERSION,
            id: state_id.clone(),
            actor_id: command.actor_id.clone(),
            state_kind: "read".to_string(),
            target_kind: command.target_kind.clone(),
            target_id: command.target_id.clone(),
            read_position: command.read_position.clone(),
            read_at,
            created_at,
        };
        let state_event_key = self.key_builder.state_event(
            &command.actor_id,
            &command.target_kind,
            &command.target_id,
            created_at,
            &state_id,
        );
        self.put_json_create_only(&state_event_key, &state).await?;
        self.record_step(&operation.id, "state-event", &state_event_key)
            .await;

        let current_state_key = self.key_builder.state_current(
            &command.actor_id,
            &command.target_kind,
            &command.target_id,
        );
        let existing = self
            .get_current_state(&command.actor_id, &command.target_kind, &command.target_id)
            .await?;
        if existing
            .as_ref()
            .map(|existing| existing.read_at <= state.read_at)
            .unwrap_or(true)
        {
            self.put_json(&current_state_key, &state, false).await?;
            self.record_step(&operation.id, "state-current", &current_state_key)
                .await;
        }

        let result = MarkReadResult {
            operation_id: operation.id.clone(),
            state_id,
            state_event_object_key: state_event_key,
            current_state_object_key: current_state_key,
        };
        self.complete_operation(&caller_id, &command.idempotency_key, operation, &result)
            .await?;
        Ok(result)
    }

    pub async fn create_attachment(
        &self,
        command: CreateAttachmentCommand,
    ) -> Result<CreateAttachmentResult> {
        validate_create_attachment(&command)?;
        let caller_id = caller_or_default(&command.caller_id);
        let _idempotency_guard = self
            .lock_idempotency(&caller_id, &command.idempotency_key)
            .await;
        let mut created_at = self.now();
        let mut entity_ids = HashMap::new();
        entity_ids.insert("attachmentId".to_string(), self.id_generator.new_id()?);

        let (operation, completed) = self
            .begin_operation(&caller_id, &command.idempotency_key, entity_ids, created_at)
            .await?;
        if completed {
            return decode_operation_result(operation.result);
        }

        let attachment_id = required_entity_id(&operation.entity_ids, "attachmentId")?;
        created_at = operation.created_at;
        let normalized_file_name = normalize_external_id(&command.original_file_name);
        let attachment_object_key = if command.object_key.trim().is_empty() {
            self.key_builder
                .attachment_object(&attachment_id, created_at, &normalized_file_name)
        } else {
            command.object_key.trim().to_string()
        };
        let metadata = AttachmentMetadata {
            schema_version: SCHEMA_VERSION,
            id: attachment_id.clone(),
            object_key: attachment_object_key.clone(),
            original_file_name: command.original_file_name.clone(),
            normalized_file_name,
            content_type: command.content_type.clone(),
            size: command.size,
            checksum: command.checksum.clone(),
            created_at,
        };
        let metadata_key = self
            .key_builder
            .attachment_metadata(&attachment_id, created_at);
        self.put_json_create_only(&metadata_key, &metadata).await?;
        self.record_step(&operation.id, "attachment-metadata", &metadata_key)
            .await;

        let lookup_key = self.key_builder.attachment_lookup(&attachment_id);
        let lookup = LookupReference {
            schema_version: SCHEMA_VERSION,
            entity_kind: "attachment".to_string(),
            entity_id: attachment_id.clone(),
            object_key: metadata_key.clone(),
            created_at,
        };
        self.put_json_create_only(&lookup_key, &lookup).await?;
        self.record_step(&operation.id, "attachment-lookup", &lookup_key)
            .await;

        let result = CreateAttachmentResult {
            operation_id: operation.id.clone(),
            attachment_id,
            attachment_metadata_key: metadata_key,
            attachment_lookup_key: lookup_key,
            attachment_object_key,
        };
        self.complete_operation(&caller_id, &command.idempotency_key, operation, &result)
            .await?;
        Ok(result)
    }

    pub async fn get_attachment(&self, attachment_id_or_key: &str) -> Result<AttachmentMetadata> {
        let target = attachment_id_or_key.trim();
        if target.is_empty() {
            return Err(validation_error("attachment id or key is required"));
        }
        let object_key = if !target.starts_with("attachments/") && !target.contains("/attachments/")
        {
            let lookup: LookupReference = self
                .get_json(&self.key_builder.attachment_lookup(target))
                .await?;
            lookup.object_key
        } else {
            target.to_string()
        };
        self.get_json(&object_key).await
    }

    fn now(&self) -> DateTime<Utc> {
        (self.clock)().with_timezone(&Utc)
    }

    async fn lock_idempotency(
        &self,
        caller_id: &str,
        idempotency_key: &str,
    ) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        let idempotency_key = idempotency_key.trim();
        if idempotency_key.is_empty() {
            return None;
        }
        let mut hasher = DefaultHasher::new();
        caller_id.hash(&mut hasher);
        idempotency_key.hash(&mut hasher);
        let shard = hasher.finish() as usize % self.idempotency_locks.len();
        Some(self.idempotency_locks[shard].clone().lock_owned().await)
    }

    fn message_entity_ids(&self, command: &SendMessageCommand) -> Result<HashMap<String, String>> {
        let mut entity_ids = HashMap::new();
        entity_ids.insert("messageId".to_string(), self.id_generator.new_id()?);
        entity_ids.insert("sentRefId".to_string(), self.id_generator.new_id()?);
        for index in 0..command.recipient_actor_ids.len() {
            entity_ids.insert(format!("inboxRefId:{index}"), self.id_generator.new_id()?);
        }
        if command.thread_id.is_empty()
            && (command.create_thread || !command.parent_message_id.is_empty())
        {
            entity_ids.insert("threadId".to_string(), self.id_generator.new_id()?);
        }
        if !command.thread_id.is_empty()
            || command.create_thread
            || !command.parent_message_id.is_empty()
        {
            entity_ids.insert("threadRefId".to_string(), self.id_generator.new_id()?);
        }
        Ok(entity_ids)
    }

    async fn begin_operation(
        &self,
        caller_id: &str,
        idempotency_key: &str,
        entity_ids: HashMap<String, String>,
        created_at: DateTime<Utc>,
    ) -> Result<(OperationRecord, bool)> {
        let operation_id = self.id_generator.new_id()?;
        let operation = OperationRecord {
            schema_version: SCHEMA_VERSION,
            id: operation_id,
            caller_id: caller_id.to_string(),
            idempotency_key: idempotency_key.to_string(),
            status: "pending".to_string(),
            entity_ids,
            result: None,
            created_at,
            updated_at: created_at,
        };

        if idempotency_key.trim().is_empty() {
            let _ = self
                .put_json(
                    &self.key_builder.operation_started(&operation.id),
                    &operation,
                    true,
                )
                .await;
            return Ok((operation, false));
        }

        let idempotency_object_key = self.key_builder.operation_id(idempotency_key, caller_id);
        match self
            .put_json(&idempotency_object_key, &operation, true)
            .await
        {
            Ok(()) => {
                let _ = self
                    .put_json(
                        &self.key_builder.operation_started(&operation.id),
                        &operation,
                        true,
                    )
                    .await;
                Ok((operation, false))
            }
            Err(ServiceError::ObjectAlreadyExists) => {
                let existing: OperationRecord = self.get_json(&idempotency_object_key).await?;
                let completed = existing.status == "completed";
                Ok((existing, completed))
            }
            Err(error) => Err(error),
        }
    }

    async fn complete_operation<T: Serialize>(
        &self,
        caller_id: &str,
        idempotency_key: &str,
        operation: OperationRecord,
        result: &T,
    ) -> Result<()> {
        let result_value = serde_json::to_value(result)?;
        let mut completed = operation;
        completed.status = "completed".to_string();
        completed.result = Some(result_value);
        completed.updated_at = self.now();

        self.put_json(
            &self.key_builder.operation_completed(&completed.id),
            &completed,
            false,
        )
        .await?;
        if !idempotency_key.trim().is_empty() {
            self.put_json(
                &self.key_builder.operation_id(idempotency_key, caller_id),
                &completed,
                false,
            )
            .await?;
        }
        Ok(())
    }

    async fn record_step(&self, operation_id: &str, step_id: &str, object_key: &str) {
        if operation_id.is_empty() || object_key.is_empty() {
            return;
        }
        let step = OperationStep {
            schema_version: SCHEMA_VERSION,
            operation_id: operation_id.to_string(),
            step_id: step_id.to_string(),
            object_key: object_key.to_string(),
            created_at: self.now(),
        };
        let _ = self
            .put_json(
                &self.key_builder.operation_step(operation_id, step_id),
                &step,
                false,
            )
            .await;
    }

    async fn scan_references<F>(
        &self,
        cursor: Cursor,
        limit: usize,
        prefix_for_window: F,
    ) -> Result<(Vec<MessageReference>, String)>
    where
        F: Fn(DateTime<Utc>) -> String,
    {
        let mut remaining_windows = self.read_lookback_minutes;
        let mut window = cursors::truncate_to_minute(cursor.window);
        let mut last_object_key = cursor.last_object_key;
        let mut last_read_key = String::new();
        let mut next_window = None;
        let mut next_last_key = String::new();
        let mut references = Vec::with_capacity(limit);

        while remaining_windows > 0 && references.len() < limit {
            let prefix = prefix_for_window(window);
            let page = self
                .store
                .list(ListInput {
                    prefix,
                    start_after: last_object_key.clone(),
                    limit: limit - references.len(),
                })
                .await?;

            for object in page.objects {
                let reference: MessageReference = self.get_json(&object.key).await?;
                references.push(reference);
                last_read_key = object.key;
                if references.len() >= limit {
                    break;
                }
            }

            if references.len() >= limit {
                if page.has_more {
                    next_window = Some(window);
                    next_last_key = last_read_key.clone();
                } else {
                    next_window = Some(cursors::next_window(window, cursor.direction));
                }
                break;
            }

            window = cursors::next_window(window, cursor.direction);
            last_object_key.clear();
            remaining_windows -= 1;
        }

        if references.is_empty() && remaining_windows == 0 {
            return Ok((references, String::new()));
        }
        if references.len() < limit && remaining_windows == 0 {
            return Ok((references, String::new()));
        }

        let next = Cursor {
            version: cursors::VERSION,
            kind: cursor.kind,
            owner: cursor.owner,
            direction: cursor.direction,
            window: next_window.unwrap_or(window),
            last_object_key: next_last_key,
            page_size: limit,
        };
        Ok((references, cursors::encode(next)?))
    }

    fn cursor_or_initial(
        &self,
        raw_cursor: &str,
        kind: &str,
        owner: &str,
        direction: Direction,
        initial_window: DateTime<Utc>,
        page_size: usize,
    ) -> Result<Cursor> {
        if raw_cursor.is_empty() {
            return Ok(cursors::initial(
                kind,
                owner,
                direction,
                initial_window,
                page_size,
            ));
        }
        let cursor = cursors::decode(raw_cursor)?;
        if cursor.kind != kind || cursor.owner != owner {
            return Err(ServiceError::InvalidCursor);
        }
        Ok(cursor)
    }

    fn normalize_limit(&self, limit: usize) -> usize {
        if limit == 0 {
            self.max_page_size
        } else {
            limit.min(self.max_page_size)
        }
    }

    async fn get_current_state(
        &self,
        actor_id: &str,
        target_kind: &str,
        target_id: &str,
    ) -> Result<Option<State>> {
        match self
            .get_json(
                &self
                    .key_builder
                    .state_current(actor_id, target_kind, target_id),
            )
            .await
        {
            Ok(state) => Ok(Some(state)),
            Err(ServiceError::ObjectNotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn put_json_create_only<T: Serialize>(&self, object_key: &str, value: &T) -> Result<()> {
        match self.put_json(object_key, value, true).await {
            Ok(()) | Err(ServiceError::ObjectAlreadyExists) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn put_json<T: Serialize>(
        &self,
        object_key: &str,
        value: &T,
        create_only: bool,
    ) -> Result<()> {
        let mut data = serde_json::to_vec_pretty(value)?;
        data.push(b'\n');
        let options = PutOptions {
            create_only,
            content_type: "application/json".to_string(),
        };
        if create_only && !self.store.capabilities().create_if_absent_atomic {
            let _guard = self.non_atomic_create_lock.lock().await;
            return self.store.put(object_key, &data, options).await;
        }
        self.store.put(object_key, &data, options).await
    }

    async fn get_json<T: DeserializeOwned>(&self, object_key: &str) -> Result<T> {
        let data = self.store.get(object_key).await?;
        Ok(serde_json::from_slice(&data)?)
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SendMessageCommand {
    #[serde(default)]
    pub caller_id: String,
    #[serde(default)]
    pub idempotency_key: String,
    #[serde(default)]
    pub sender_actor_id: String,
    #[serde(default)]
    pub recipient_actor_ids: Vec<String>,
    #[serde(default)]
    pub message_type: String,
    #[serde(default)]
    pub payload: Option<Value>,
    #[serde(default)]
    pub attachment_ids: Vec<String>,
    #[serde(default)]
    pub thread_id: String,
    #[serde(default)]
    pub parent_message_id: String,
    #[serde(default)]
    pub create_thread: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageResult {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub operation_id: String,
    pub message_id: String,
    pub message_object_key: String,
    pub message_lookup_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub thread_id: String,
    pub reference_keys: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MailboxItem {
    pub reference: MessageReference,
    pub message: Message,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_state: Option<State>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListMailboxResult {
    pub items: Vec<MailboxItem>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub next_cursor: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadItem {
    pub reference: MessageReference,
    pub message: Message,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListThreadResult {
    pub thread: Thread,
    pub items: Vec<ThreadItem>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub next_cursor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_state: Option<State>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SendBroadcastCommand {
    #[serde(default)]
    pub caller_id: String,
    #[serde(default)]
    pub idempotency_key: String,
    #[serde(default)]
    pub sender_actor_id: String,
    #[serde(default)]
    pub audience_type: String,
    #[serde(default)]
    pub audience_keys: Vec<String>,
    #[serde(default)]
    pub message_type: String,
    #[serde(default)]
    pub payload: Option<Value>,
    #[serde(default)]
    pub attachment_ids: Vec<String>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SendBroadcastResult {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub operation_id: String,
    pub broadcast_id: String,
    pub broadcast_object_key: String,
    pub broadcast_lookup_key: String,
    pub audience_object_keys: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MarkReadCommand {
    #[serde(default)]
    pub caller_id: String,
    #[serde(default)]
    pub idempotency_key: String,
    #[serde(default)]
    pub actor_id: String,
    #[serde(default)]
    pub target_kind: String,
    #[serde(default)]
    pub target_id: String,
    #[serde(default)]
    pub read_position: String,
    #[serde(default)]
    pub read_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MarkReadResult {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub operation_id: String,
    pub state_id: String,
    pub state_event_object_key: String,
    pub current_state_object_key: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateAttachmentCommand {
    #[serde(default)]
    pub caller_id: String,
    #[serde(default)]
    pub idempotency_key: String,
    #[serde(default)]
    pub object_key: String,
    #[serde(default)]
    pub original_file_name: String,
    #[serde(default)]
    pub content_type: String,
    #[serde(default)]
    pub size: i64,
    #[serde(default)]
    pub checksum: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreateAttachmentResult {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub operation_id: String,
    pub attachment_id: String,
    pub attachment_metadata_key: String,
    pub attachment_lookup_key: String,
    pub attachment_object_key: String,
}

fn validate_send_message(command: &SendMessageCommand) -> Result<()> {
    if command.sender_actor_id.trim().is_empty() {
        return Err(validation_error("senderActorId is required"));
    }
    if command.recipient_actor_ids.is_empty() {
        return Err(validation_error(
            "recipientActorIds must contain at least one actor",
        ));
    }
    if command
        .recipient_actor_ids
        .iter()
        .any(|actor_id| actor_id.trim().is_empty())
    {
        return Err(validation_error(
            "recipientActorIds cannot contain empty actor ids",
        ));
    }
    if command.message_type.trim().is_empty() {
        return Err(validation_error("messageType is required"));
    }
    Ok(())
}

fn validate_send_broadcast(command: &SendBroadcastCommand) -> Result<()> {
    if command.sender_actor_id.trim().is_empty() {
        return Err(validation_error("senderActorId is required"));
    }
    if command.audience_type != "all"
        && command.audience_type != "tag"
        && command.audience_type != "explicit"
    {
        return Err(validation_error(
            "audienceType must be all, tag, or explicit",
        ));
    }
    if command.audience_type != "all" && command.audience_keys.is_empty() {
        return Err(validation_error(
            "audienceKeys are required for tag and explicit broadcasts",
        ));
    }
    if command.message_type.trim().is_empty() {
        return Err(validation_error("messageType is required"));
    }
    Ok(())
}

fn validate_mark_read(command: &MarkReadCommand) -> Result<()> {
    if command.actor_id.trim().is_empty() {
        return Err(validation_error("actorId is required"));
    }
    if command.target_kind != "messages" && command.target_kind != "threads" {
        return Err(validation_error("targetKind must be messages or threads"));
    }
    if command.target_id.trim().is_empty() {
        return Err(validation_error("targetId is required"));
    }
    Ok(())
}

fn validate_create_attachment(command: &CreateAttachmentCommand) -> Result<()> {
    if command.original_file_name.trim().is_empty() {
        return Err(validation_error("originalFileName is required"));
    }
    if command.content_type.trim().is_empty() {
        return Err(validation_error("contentType is required"));
    }
    if command.size < 0 {
        return Err(validation_error("size cannot be negative"));
    }
    Ok(())
}

fn validation_error(message: &str) -> ServiceError {
    ServiceError::Validation(message.to_string())
}

fn caller_or_default(caller_id: &str) -> String {
    let trimmed = caller_id.trim();
    if trimmed.is_empty() {
        DEFAULT_CALLER_ID.to_string()
    } else {
        trimmed.to_string()
    }
}

fn normalized_payload(payload: Option<Value>) -> Value {
    payload.unwrap_or_else(|| json!({}))
}

fn normalize_audience_keys(audience_type: &str, audience_keys: &[String]) -> Vec<String> {
    if audience_type == "all" {
        return vec!["all".to_string()];
    }
    let mut normalized = Vec::with_capacity(audience_keys.len());
    let mut seen = HashSet::new();
    for audience_key in audience_keys {
        let trimmed = audience_key.trim();
        if trimmed.is_empty() || seen.contains(trimmed) {
            continue;
        }
        seen.insert(trimmed.to_string());
        normalized.push(trimmed.to_string());
    }
    normalized
}

fn required_entity_id(entity_ids: &HashMap<String, String>, key: &str) -> Result<String> {
    entity_ids
        .get(key)
        .cloned()
        .ok_or_else(|| ServiceError::Storage(format!("operation is missing entity id {key}")))
}

fn decode_operation_result<T: DeserializeOwned>(result: Option<Value>) -> Result<T> {
    let value = result.ok_or_else(|| {
        ServiceError::Storage("completed operation is missing result".to_string())
    })?;
    Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration as StdDuration;

    use async_trait::async_trait;
    use chrono::{Duration, TimeZone};

    use super::*;
    use crate::storage::localfs::LocalFileStore;
    use crate::storage::{ListPage, ListedObject, ObjectInfo, StoreCapabilities};

    #[tokio::test]
    async fn message_mailbox_thread_state_attachment_and_broadcast() {
        let fixed_now = Utc.with_ymd_and_hms(2026, 6, 1, 11, 22, 33).unwrap();
        let (_temp_dir, service) = new_test_service(fixed_now);

        let send_result = service
            .send_message(SendMessageCommand {
                caller_id: "tests".to_string(),
                idempotency_key: "send-1".to_string(),
                sender_actor_id: "actor-a".to_string(),
                recipient_actor_ids: vec!["actor-b".to_string()],
                message_type: "text".to_string(),
                payload: Some(json!({"text": "hello"})),
                create_thread: true,
                ..Default::default()
            })
            .await
            .expect("send");
        assert!(!send_result.message_id.is_empty());
        assert!(!send_result.message_object_key.is_empty());
        assert!(!send_result.thread_id.is_empty());

        let retry_result = service
            .send_message(SendMessageCommand {
                caller_id: "tests".to_string(),
                idempotency_key: "send-1".to_string(),
                sender_actor_id: "actor-a".to_string(),
                recipient_actor_ids: vec!["actor-b".to_string()],
                message_type: "text".to_string(),
                payload: Some(json!({"text": "hello"})),
                create_thread: true,
                ..Default::default()
            })
            .await
            .expect("retry");
        assert_eq!(retry_result.message_id, send_result.message_id);

        let message = service
            .get_message(&send_result.message_id)
            .await
            .expect("message");
        assert_eq!(message.sender_actor_id, "actor-a");
        assert_eq!(message.thread_id, send_result.thread_id);

        let mailbox = service
            .list_mailbox("actor-b", "inbox", "", 10)
            .await
            .expect("mailbox");
        assert_eq!(mailbox.items.len(), 1);
        assert_eq!(mailbox.items[0].message.id, send_result.message_id);

        let mark_read_result = service
            .mark_read(MarkReadCommand {
                caller_id: "tests".to_string(),
                idempotency_key: "read-1".to_string(),
                actor_id: "actor-b".to_string(),
                target_kind: "messages".to_string(),
                target_id: send_result.message_id.clone(),
                read_at: Some(fixed_now + Duration::seconds(1)),
                ..Default::default()
            })
            .await
            .expect("mark read");
        assert!(!mark_read_result.state_id.is_empty());
        assert!(!mark_read_result.current_state_object_key.is_empty());

        let mailbox_with_state = service
            .list_mailbox("actor-b", "inbox", "", 10)
            .await
            .expect("mailbox with state");
        assert!(mailbox_with_state.items[0].read_state.is_some());

        let thread = service
            .list_thread(&send_result.thread_id, "actor-b", "", 10)
            .await
            .expect("thread");
        assert_eq!(thread.items.len(), 1);
        assert_eq!(thread.items[0].message.id, send_result.message_id);

        let attachment = service
            .create_attachment(CreateAttachmentCommand {
                caller_id: "tests".to_string(),
                idempotency_key: "attachment-1".to_string(),
                original_file_name: "Report Final.pdf".to_string(),
                content_type: "application/pdf".to_string(),
                size: 1234,
                checksum: "sha256:test".to_string(),
                ..Default::default()
            })
            .await
            .expect("attachment");
        let metadata = service
            .get_attachment(&attachment.attachment_id)
            .await
            .expect("metadata");
        assert_eq!(metadata.normalized_file_name, "report-final.pdf");

        let broadcast_result = service
            .send_broadcast(SendBroadcastCommand {
                caller_id: "tests".to_string(),
                idempotency_key: "broadcast-1".to_string(),
                sender_actor_id: "system".to_string(),
                audience_type: "tag".to_string(),
                audience_keys: vec!["beta".to_string()],
                message_type: "text".to_string(),
                payload: Some(json!({"text": "announcement"})),
                ..Default::default()
            })
            .await
            .expect("broadcast");
        let broadcast = service
            .get_broadcast(&broadcast_result.broadcast_id)
            .await
            .expect("get broadcast");
        assert_eq!(broadcast.audience_type, "tag");
        assert_eq!(broadcast.audience_keys[0], "beta");
    }

    #[tokio::test]
    async fn list_mailbox_uses_prefix_window_cursor() {
        let fixed_now = Utc.with_ymd_and_hms(2026, 6, 1, 11, 22, 33).unwrap();
        let (_temp_dir, service) = new_test_service(fixed_now);

        for _ in 0..3 {
            service
                .send_message(SendMessageCommand {
                    sender_actor_id: "actor-a".to_string(),
                    recipient_actor_ids: vec!["actor-b".to_string()],
                    message_type: "text".to_string(),
                    payload: Some(json!({"text": "page"})),
                    ..Default::default()
                })
                .await
                .expect("send");
        }

        let first_page = service
            .list_mailbox("actor-b", "inbox", "", 2)
            .await
            .expect("first page");
        assert_eq!(first_page.items.len(), 2);
        assert!(!first_page.next_cursor.is_empty());

        let second_page = service
            .list_mailbox("actor-b", "inbox", &first_page.next_cursor, 2)
            .await
            .expect("second page");
        assert_eq!(second_page.items.len(), 1);
    }

    #[tokio::test]
    async fn non_atomic_store_create_only_writes_are_serialized() {
        let fixed_now = Utc.with_ymd_and_hms(2026, 6, 1, 11, 22, 33).unwrap();
        let store = Arc::new(InstrumentedNonAtomicStore::default());
        let service = new_service_with_store(store.clone(), fixed_now);

        let left_command = SendMessageCommand {
            sender_actor_id: "actor-a".to_string(),
            recipient_actor_ids: vec!["actor-b".to_string()],
            message_type: "text".to_string(),
            payload: Some(json!({"text": "left"})),
            ..Default::default()
        };
        let right_command = SendMessageCommand {
            sender_actor_id: "actor-c".to_string(),
            recipient_actor_ids: vec!["actor-d".to_string()],
            message_type: "text".to_string(),
            payload: Some(json!({"text": "right"})),
            ..Default::default()
        };

        let (left, right) = tokio::join!(
            service.send_message(left_command),
            service.send_message(right_command)
        );
        left.expect("left send");
        right.expect("right send");
        assert_eq!(store.max_active_create_only.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_idempotent_requests_share_completed_result() {
        let fixed_now = Utc.with_ymd_and_hms(2026, 6, 1, 11, 22, 33).unwrap();
        let store = Arc::new(InstrumentedNonAtomicStore::default());
        let service = new_service_with_store(store, fixed_now);
        let command = SendMessageCommand {
            caller_id: "tests".to_string(),
            idempotency_key: "same-message".to_string(),
            sender_actor_id: "actor-a".to_string(),
            recipient_actor_ids: vec!["actor-b".to_string()],
            message_type: "text".to_string(),
            payload: Some(json!({"text": "hello"})),
            create_thread: true,
            ..Default::default()
        };

        let (left, right) = tokio::join!(
            service.send_message(command.clone()),
            service.send_message(command)
        );
        let left = left.expect("left send");
        let right = right.expect("right send");

        assert_eq!(left.message_id, right.message_id);
        assert_eq!(left.thread_id, right.thread_id);
    }

    fn new_test_service(now: DateTime<Utc>) -> (tempfile::TempDir, Service) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileStore::new(temp_dir.path()).expect("store"));
        let service = new_service_with_store(store, now);
        (temp_dir, service)
    }

    fn new_service_with_store(store: Arc<dyn ObjectStore>, now: DateTime<Utc>) -> Service {
        Service::new(ServiceOptions {
            store,
            key_builder: KeyBuilder::new(""),
            id_generator: IdGenerator::new(),
            clock: Some(Arc::new(move || now)),
            max_page_size: 50,
            read_lookback_minutes: 120,
        })
    }

    #[derive(Default)]
    struct InstrumentedNonAtomicStore {
        objects: StdMutex<HashMap<String, Vec<u8>>>,
        active_create_only: AtomicUsize,
        max_active_create_only: AtomicUsize,
    }

    #[async_trait]
    impl ObjectStore for InstrumentedNonAtomicStore {
        fn capabilities(&self) -> StoreCapabilities {
            StoreCapabilities {
                create_if_absent_atomic: false,
            }
        }

        async fn put(&self, key: &str, data: &[u8], options: PutOptions) -> Result<()> {
            if options.create_only {
                let active = self.active_create_only.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active_create_only
                    .fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(StdDuration::from_millis(10)).await;
            }

            let result = {
                let mut objects = self.objects.lock().expect("objects lock poisoned");
                if options.create_only && objects.contains_key(key) {
                    Err(ServiceError::ObjectAlreadyExists)
                } else {
                    objects.insert(key.to_string(), data.to_vec());
                    Ok(())
                }
            };

            if options.create_only {
                self.active_create_only.fetch_sub(1, Ordering::SeqCst);
            }
            result
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>> {
            self.objects
                .lock()
                .expect("objects lock poisoned")
                .get(key)
                .cloned()
                .ok_or(ServiceError::ObjectNotFound)
        }

        async fn head(&self, key: &str) -> Result<ObjectInfo> {
            let objects = self.objects.lock().expect("objects lock poisoned");
            let data = objects.get(key).ok_or(ServiceError::ObjectNotFound)?;
            Ok(ObjectInfo {
                key: key.to_string(),
                size: data.len() as u64,
                content_type: String::new(),
                modified_at: Utc::now(),
            })
        }

        async fn list(&self, mut input: ListInput) -> Result<ListPage> {
            if input.limit == 0 {
                input.limit = 100;
            }
            let objects = self.objects.lock().expect("objects lock poisoned");
            let mut listed = objects
                .iter()
                .filter(|(key, _)| key.starts_with(&input.prefix))
                .filter(|(key, _)| input.start_after.is_empty() || *key > &input.start_after)
                .map(|(key, data)| ListedObject {
                    key: key.clone(),
                    size: data.len() as u64,
                    modified_at: Utc::now(),
                })
                .collect::<Vec<_>>();
            listed.sort_by(|left, right| left.key.cmp(&right.key));
            let has_more = listed.len() > input.limit;
            if has_more {
                listed.truncate(input.limit);
            }
            let next_after_key = listed
                .last()
                .map(|object| object.key.clone())
                .unwrap_or_default();
            Ok(ListPage {
                objects: listed,
                has_more,
                next_after_key,
            })
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.objects
                .lock()
                .expect("objects lock poisoned")
                .remove(key)
                .map(|_| ())
                .ok_or(ServiceError::ObjectNotFound)
        }
    }
}
