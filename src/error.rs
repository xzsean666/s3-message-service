use thiserror::Error;

pub type Result<T> = std::result::Result<T, ServiceError>;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("invalid cursor")]
    InvalidCursor,
    #[error("object already exists")]
    ObjectAlreadyExists,
    #[error("object not found")]
    ObjectNotFound,
    #[error("invalid object key")]
    InvalidObjectKey,
    #[error("storage error: {0}")]
    Storage(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("id generation error: {0}")]
    IdGeneration(String),
}
