use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

const MAX_TIMESTAMP_MILLIS: i64 = 9_999_999_999_999_999;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyBuilder {
    namespace: String,
}

impl KeyBuilder {
    pub fn new(namespace: &str) -> Self {
        Self {
            namespace: normalize_namespace(namespace),
        }
    }

    pub fn message_body(&self, message_id: &str, created_at: DateTime<Utc>) -> String {
        self.with_namespace(&format!(
            "messages/{}/{}.json",
            time_prefix(created_at),
            message_id
        ))
    }

    pub fn message_lookup(&self, message_id: &str) -> String {
        self.with_namespace(&format!("messages/by-id/{}.json", message_id))
    }

    pub fn attachment_metadata(&self, attachment_id: &str, created_at: DateTime<Utc>) -> String {
        self.with_namespace(&format!(
            "attachments/metadata/{}/{}.json",
            time_prefix(created_at),
            attachment_id
        ))
    }

    pub fn attachment_lookup(&self, attachment_id: &str) -> String {
        self.with_namespace(&format!("attachments/by-id/{}.json", attachment_id))
    }

    pub fn attachment_object(
        &self,
        attachment_id: &str,
        created_at: DateTime<Utc>,
        normalized_file_name: &str,
    ) -> String {
        self.with_namespace(&format!(
            "attachments/objects/{}/{}/{}",
            time_prefix(created_at),
            attachment_id,
            normalize_external_id(normalized_file_name)
        ))
    }

    pub fn mailbox_reference(
        &self,
        actor_id: &str,
        direction: &str,
        created_at: DateTime<Utc>,
        message_id: &str,
        reference_id: &str,
    ) -> String {
        self.with_namespace(&format!(
            "mailboxes/{}/{}/{}/{}_{}_{}_{}.json",
            normalize_external_id(actor_id),
            normalize_external_id(direction),
            time_prefix(created_at),
            feed_sort_key(created_at),
            compact_timestamp(created_at),
            message_id,
            reference_id
        ))
    }

    pub fn mailbox_prefix(&self, actor_id: &str, direction: &str, window: DateTime<Utc>) -> String {
        self.with_namespace(&format!(
            "mailboxes/{}/{}/{}/",
            normalize_external_id(actor_id),
            normalize_external_id(direction),
            time_prefix(window)
        ))
    }

    pub fn thread_metadata(&self, thread_id: &str) -> String {
        self.with_namespace(&format!("threads/{}/metadata.json", thread_id))
    }

    pub fn thread_reference(
        &self,
        thread_id: &str,
        created_at: DateTime<Utc>,
        message_id: &str,
        reference_id: &str,
    ) -> String {
        self.with_namespace(&format!(
            "threads/{}/messages/{}/{}_{}_{}_{}.json",
            thread_id,
            time_prefix(created_at),
            thread_sort_key(created_at),
            compact_timestamp(created_at),
            message_id,
            reference_id
        ))
    }

    pub fn thread_prefix(&self, thread_id: &str, window: DateTime<Utc>) -> String {
        self.with_namespace(&format!(
            "threads/{}/messages/{}/",
            thread_id,
            time_prefix(window)
        ))
    }

    pub fn broadcast_body(&self, broadcast_id: &str, created_at: DateTime<Utc>) -> String {
        self.with_namespace(&format!(
            "broadcast/messages/{}/{}.json",
            time_prefix(created_at),
            broadcast_id
        ))
    }

    pub fn broadcast_lookup(&self, broadcast_id: &str) -> String {
        self.with_namespace(&format!("broadcast/by-id/{}.json", broadcast_id))
    }

    pub fn broadcast_audience(
        &self,
        audience_type: &str,
        audience_key: &str,
        created_at: DateTime<Utc>,
        broadcast_id: &str,
    ) -> String {
        self.with_namespace(&format!(
            "broadcast/audiences/{}/{}/{}/{}_{}.json",
            normalize_external_id(audience_type),
            normalize_external_id(audience_key),
            time_prefix(created_at),
            feed_sort_key(created_at),
            broadcast_id
        ))
    }

    pub fn broadcast_audience_prefix(
        &self,
        audience_type: &str,
        audience_key: &str,
        window: DateTime<Utc>,
    ) -> String {
        self.with_namespace(&format!(
            "broadcast/audiences/{}/{}/{}/",
            normalize_external_id(audience_type),
            normalize_external_id(audience_key),
            time_prefix(window)
        ))
    }

    pub fn state_event(
        &self,
        actor_id: &str,
        target_kind: &str,
        target_id: &str,
        created_at: DateTime<Utc>,
        state_id: &str,
    ) -> String {
        self.with_namespace(&format!(
            "states/{}/events/{}/{}/{}/{}_{}.json",
            normalize_external_id(actor_id),
            normalize_external_id(target_kind),
            normalize_external_id(target_id),
            time_prefix(created_at),
            feed_sort_key(created_at),
            state_id
        ))
    }

    pub fn state_current(&self, actor_id: &str, target_kind: &str, target_id: &str) -> String {
        self.with_namespace(&format!(
            "states/{}/current/{}/{}.json",
            normalize_external_id(actor_id),
            normalize_external_id(target_kind),
            normalize_external_id(target_id)
        ))
    }

    pub fn operation_id(&self, idempotency_key: &str, caller_id: &str) -> String {
        self.with_namespace(&format!(
            "operations/idempotency/{}/{}.json",
            normalize_external_id(caller_id),
            normalize_external_id(idempotency_key)
        ))
    }

    pub fn operation_started(&self, operation_id: &str) -> String {
        self.with_namespace(&format!("operations/by-id/{}/started.json", operation_id))
    }

    pub fn operation_step(&self, operation_id: &str, step_id: &str) -> String {
        self.with_namespace(&format!(
            "operations/by-id/{}/steps/{}.json",
            operation_id,
            normalize_external_id(step_id)
        ))
    }

    pub fn operation_completed(&self, operation_id: &str) -> String {
        self.with_namespace(&format!("operations/by-id/{}/completed.json", operation_id))
    }

    fn with_namespace(&self, key: &str) -> String {
        if self.namespace.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.namespace, key.trim_start_matches('/'))
        }
    }
}

pub fn time_prefix(instant: DateTime<Utc>) -> String {
    instant
        .with_timezone(&Utc)
        .format("year=%Y/month=%m/day=%d/hour=%H/minute=%M")
        .to_string()
}

pub fn compact_timestamp(instant: DateTime<Utc>) -> String {
    instant
        .with_timezone(&Utc)
        .format("%Y%m%dT%H%M%S.%fZ")
        .to_string()
}

pub fn feed_sort_key(instant: DateTime<Utc>) -> String {
    format!(
        "{:016}",
        MAX_TIMESTAMP_MILLIS - instant.with_timezone(&Utc).timestamp_millis()
    )
}

pub fn thread_sort_key(instant: DateTime<Utc>) -> String {
    format!("{:016}", instant.with_timezone(&Utc).timestamp_millis())
}

pub fn normalize_external_id(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return format!("empty-{}", short_hash(raw));
    }

    let mut normalized = String::new();
    let mut previous_dash = false;
    for character in trimmed.chars() {
        for lowered in character.to_lowercase() {
            let valid = lowered.is_alphanumeric() || matches!(lowered, '-' | '_' | '.');
            if valid {
                normalized.push(lowered);
                previous_dash = false;
            } else if !previous_dash {
                normalized.push('-');
                previous_dash = true;
            }
        }
    }

    let normalized = normalized.trim_matches(['-', '.']).to_string();
    let mut normalized = if normalized.is_empty() {
        "id".to_string()
    } else {
        normalized
    };
    if normalized.len() > 96 {
        let prefix: String = normalized.chars().take(72).collect();
        normalized = format!("{}-{}", prefix, short_hash(trimmed));
    }
    normalized
}

fn normalize_namespace(raw: &str) -> String {
    let trimmed = raw.trim().trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        normalize_external_id(trimmed)
    }
}

fn short_hash(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let digest = hasher.finalize();
    format!("{:x}", digest)[..16].to_string()
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;

    #[test]
    fn normalize_external_identifier() {
        assert_eq!(normalize_external_id(" User/ABC 01 "), "user-abc-01");
        assert!(!normalize_external_id("a/b").contains('/'));
    }

    #[test]
    fn time_prefix_and_sort_keys() {
        let older = Utc.with_ymd_and_hms(2026, 6, 1, 11, 22, 1).unwrap();
        let newer = older + Duration::minutes(1);

        assert_eq!(
            time_prefix(older),
            "year=2026/month=06/day=01/hour=11/minute=22"
        );
        assert!(feed_sort_key(newer) < feed_sort_key(older));
        assert!(thread_sort_key(older) < thread_sort_key(newer));
    }

    #[test]
    fn builder_applies_namespace() {
        let builder = KeyBuilder::new("dev/test");
        assert_eq!(
            builder.message_lookup("message-1"),
            "dev-test/messages/by-id/message-1.json"
        );
    }
}
