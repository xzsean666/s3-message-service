use std::env;

use crate::error::{Result, ServiceError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub storage_provider: String,
    pub filesystem_root: String,
    pub object_namespace: String,
    pub http_address: String,
    pub max_page_size: usize,
    pub read_lookback_minutes: usize,
}

impl Config {
    pub fn load_from_env() -> Result<Self> {
        let config = Self {
            storage_provider: get_env("S3MS_STORAGE_PROVIDER", "filesystem"),
            filesystem_root: get_env("S3MS_FILESYSTEM_ROOT", ".s3-message-data"),
            object_namespace: env::var("S3MS_OBJECT_NAMESPACE").unwrap_or_default(),
            http_address: get_env("S3MS_HTTP_ADDR", ":8080"),
            max_page_size: get_env_usize("S3MS_MAX_PAGE_SIZE", 100),
            read_lookback_minutes: get_env_usize("S3MS_READ_LOOKBACK_MINUTES", 43_200),
        };

        if config.storage_provider != "filesystem"
            && config.storage_provider != "b2"
            && config.storage_provider != "backblaze-b2"
        {
            return Err(ServiceError::Configuration(format!(
                "unsupported storage provider {:?}",
                config.storage_provider
            )));
        }
        if config.max_page_size == 0 {
            return Err(ServiceError::Configuration(
                "S3MS_MAX_PAGE_SIZE must be positive".to_string(),
            ));
        }
        if config.read_lookback_minutes == 0 {
            return Err(ServiceError::Configuration(
                "S3MS_READ_LOOKBACK_MINUTES must be positive".to_string(),
            ));
        }

        Ok(config)
    }
}

fn get_env(name: &str, fallback: &str) -> String {
    env::var(name).unwrap_or_else(|_| fallback.to_string())
}

fn get_env_usize(name: &str, fallback: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(fallback)
}
