use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub schema_version: u16,
    pub id: String,
    pub sender_actor_id: String,
    pub recipient_actor_ids: Vec<String>,
    pub message_type: String,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachment_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub parent_message_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LookupReference {
    pub schema_version: u16,
    pub entity_kind: String,
    pub entity_id: String,
    pub object_key: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MessageReference {
    pub schema_version: u16,
    pub id: String,
    pub message_id: String,
    pub message_object_key: String,
    pub owner_id: String,
    pub reference_kind: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Thread {
    pub schema_version: u16,
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub root_message_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub external_correlation_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Broadcast {
    pub schema_version: u16,
    pub id: String,
    pub sender_actor_id: String,
    pub audience_type: String,
    pub audience_keys: Vec<String>,
    pub message_type: String,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachment_ids: Vec<String>,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct State {
    pub schema_version: u16,
    pub id: String,
    pub actor_id: String,
    pub state_kind: String,
    pub target_kind: String,
    pub target_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub read_position: String,
    pub read_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentMetadata {
    pub schema_version: u16,
    pub id: String,
    pub object_key: String,
    pub original_file_name: String,
    pub normalized_file_name: String,
    pub content_type: String,
    pub size: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub checksum: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OperationRecord {
    pub schema_version: u16,
    pub id: String,
    pub caller_id: String,
    pub idempotency_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub operation_kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_hash: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub entity_ids: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OperationStep {
    pub schema_version: u16,
    pub operation_id: String,
    pub step_id: String,
    pub object_key: String,
    pub created_at: DateTime<Utc>,
}
