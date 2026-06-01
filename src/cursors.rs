use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Duration, Timelike, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Result, ServiceError};

pub const VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Direction {
    NewestFirst,
    OldestFirst,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Cursor {
    pub version: u16,
    pub kind: String,
    pub owner: String,
    pub direction: Direction,
    pub window: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_object_key: String,
    pub page_size: usize,
}

pub fn initial(
    kind: &str,
    owner: &str,
    direction: Direction,
    window: DateTime<Utc>,
    page_size: usize,
) -> Cursor {
    Cursor {
        version: VERSION,
        kind: kind.to_string(),
        owner: owner.to_string(),
        direction,
        window: truncate_to_minute(window),
        last_object_key: String::new(),
        page_size,
    }
}

pub fn encode(mut cursor: Cursor) -> Result<String> {
    cursor.version = VERSION;
    let data = serde_json::to_vec(&cursor)?;
    Ok(URL_SAFE_NO_PAD.encode(data))
}

pub fn decode(raw: &str) -> Result<Cursor> {
    if raw.is_empty() {
        return Err(ServiceError::InvalidCursor);
    }
    let data = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| ServiceError::InvalidCursor)?;
    let cursor: Cursor = serde_json::from_slice(&data).map_err(|_| ServiceError::InvalidCursor)?;
    if cursor.version != VERSION
        || cursor.kind.is_empty()
        || cursor.owner.is_empty()
        || cursor.page_size == 0
    {
        return Err(ServiceError::InvalidCursor);
    }
    Ok(cursor)
}

pub fn next_window(window: DateTime<Utc>, direction: Direction) -> DateTime<Utc> {
    match direction {
        Direction::OldestFirst => truncate_to_minute(window) + Duration::minutes(1),
        Direction::NewestFirst => truncate_to_minute(window) - Duration::minutes(1),
    }
}

pub fn truncate_to_minute(instant: DateTime<Utc>) -> DateTime<Utc> {
    instant
        .with_second(0)
        .and_then(|value| value.with_nanosecond(0))
        .expect("valid minute truncation")
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn encode_decode_cursor() {
        let window = Utc.with_ymd_and_hms(2026, 6, 1, 11, 22, 33).unwrap();
        let mut cursor = initial(
            "mailbox:inbox",
            "actor-1",
            Direction::NewestFirst,
            window,
            25,
        );
        cursor.last_object_key = "mailboxes/actor-1/inbox/key.json".to_string();

        let encoded = encode(cursor.clone()).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        assert_eq!(decoded.window.second(), 0);
        assert_eq!(decoded.last_object_key, cursor.last_object_key);
    }

    #[test]
    fn moves_to_next_window() {
        let window = Utc.with_ymd_and_hms(2026, 6, 1, 11, 22, 0).unwrap();

        assert_eq!(
            next_window(window, Direction::NewestFirst),
            window - Duration::minutes(1)
        );
        assert_eq!(
            next_window(window, Direction::OldestFirst),
            window + Duration::minutes(1)
        );
    }
}
