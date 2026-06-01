use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::Result;

pub mod b2;
pub mod localfs;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PutOptions {
    pub create_only: bool,
    pub content_type: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreCapabilities {
    pub create_if_absent_atomic: bool,
}

impl Default for StoreCapabilities {
    fn default() -> Self {
        Self {
            create_if_absent_atomic: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub content_type: String,
    pub modified_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedObject {
    pub key: String,
    pub size: u64,
    pub modified_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListInput {
    pub prefix: String,
    pub start_after: String,
    pub limit: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListPage {
    pub objects: Vec<ListedObject>,
    pub has_more: bool,
    pub next_after_key: String,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities::default()
    }

    async fn put(&self, key: &str, data: &[u8], options: PutOptions) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
    async fn head(&self, key: &str) -> Result<ObjectInfo>;
    async fn list(&self, input: ListInput) -> Result<ListPage>;
    async fn delete(&self, key: &str) -> Result<()>;
}
