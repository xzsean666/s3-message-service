use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Utc};
use reqwest::Url;
use serde::Deserialize;

use crate::error::{Result, ServiceError};
use crate::storage::{ListInput, ListPage, ListedObject, ObjectInfo, ObjectStore, PutOptions};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct B2Config {
    pub bucket_name: String,
    pub application_key_id: String,
    pub application_key: String,
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
    pub force_path_style: bool,
}

impl B2Config {
    pub fn from_env() -> Result<Self> {
        let bucket_name = required_env("B2_BUCKET_NAME")?;
        let application_key_id = required_env("B2_APPLICATION_KEY_ID")?;
        let application_key = required_env("B2_APPLICATION_KEY")?;
        let endpoint_url = optional_env("B2_S3_ENDPOINT");
        let region = optional_env("B2_S3_REGION");
        let force_path_style = optional_env("B2_FORCE_PATH_STYLE")
            .map(|value| value != "false")
            .unwrap_or(true);

        Ok(Self {
            bucket_name,
            application_key_id,
            application_key,
            endpoint_url,
            region,
            force_path_style,
        })
    }
}

#[derive(Clone)]
pub struct B2ObjectStore {
    bucket_name: String,
    client: Client,
}

impl B2ObjectStore {
    pub async fn from_config(config: B2Config) -> Result<Self> {
        let endpoint_url = match config.endpoint_url {
            Some(endpoint_url) => endpoint_url,
            None => {
                authorize_b2_s3_endpoint(&config.application_key_id, &config.application_key)
                    .await?
            }
        };
        let region = match config.region {
            Some(region) => region,
            None => region_from_endpoint(&endpoint_url)?,
        };
        let credentials = Credentials::new(
            config.application_key_id,
            config.application_key,
            None,
            None,
            "backblaze-b2",
        );
        let shared_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region))
            .endpoint_url(endpoint_url)
            .credentials_provider(credentials)
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
            .force_path_style(config.force_path_style)
            .build();

        Ok(Self {
            bucket_name: config.bucket_name,
            client: Client::from_conf(s3_config),
        })
    }
}

#[async_trait]
impl ObjectStore for B2ObjectStore {
    async fn put(&self, key: &str, data: &[u8], options: PutOptions) -> Result<()> {
        validate_object_key(key)?;
        if options.create_only {
            match self.head(key).await {
                Ok(_) => return Err(ServiceError::ObjectAlreadyExists),
                Err(ServiceError::ObjectNotFound) => {}
                Err(error) => return Err(error),
            }
        }
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(key)
            .body(ByteStream::from(data.to_vec()));
        if !options.content_type.is_empty() {
            request = request.content_type(options.content_type);
        }
        request.send().await.map_err(map_s3_error)?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        validate_object_key(key)?;
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(map_s3_error)?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|error| ServiceError::Storage(error.to_string()))?
            .into_bytes();
        Ok(bytes.to_vec())
    }

    async fn head(&self, key: &str) -> Result<ObjectInfo> {
        validate_object_key(key)?;
        let output = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(map_s3_error)?;
        Ok(ObjectInfo {
            key: key.to_string(),
            size: output.content_length().unwrap_or_default().max(0) as u64,
            content_type: output.content_type().unwrap_or_default().to_string(),
            modified_at: Utc::now(),
        })
    }

    async fn list(&self, mut input: ListInput) -> Result<ListPage> {
        if input.limit == 0 {
            input.limit = 100;
        }
        let max_keys = input.limit.min(1000) as i32;
        let mut request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .prefix(input.prefix)
            .max_keys(max_keys);
        if !input.start_after.is_empty() {
            request = request.start_after(input.start_after);
        }
        let output = request.send().await.map_err(map_s3_error)?;
        let mut objects = Vec::new();
        for object in output.contents() {
            let Some(key) = object.key() else {
                continue;
            };
            objects.push(ListedObject {
                key: key.to_string(),
                size: object.size().unwrap_or_default().max(0) as u64,
                modified_at: DateTime::<Utc>::UNIX_EPOCH,
            });
        }
        let next_after_key = objects
            .last()
            .map(|object| object.key.clone())
            .unwrap_or_default();
        Ok(ListPage {
            objects,
            has_more: output.is_truncated().unwrap_or(false),
            next_after_key,
        })
    }

    async fn delete(&self, key: &str) -> Result<()> {
        validate_object_key(key)?;
        self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(map_s3_error)?;
        Ok(())
    }
}

async fn authorize_b2_s3_endpoint(
    application_key_id: &str,
    application_key: &str,
) -> Result<String> {
    let response = reqwest::Client::new()
        .get("https://api.backblazeb2.com/b2api/v4/b2_authorize_account")
        .basic_auth(application_key_id, Some(application_key))
        .send()
        .await
        .map_err(|error| ServiceError::Storage(format!("B2 authorize request failed: {error}")))?;
    if !response.status().is_success() {
        return Err(ServiceError::Storage(format!(
            "B2 authorize request failed with HTTP {}",
            response.status()
        )));
    }
    let body: AuthorizeAccountResponse = response.json().await.map_err(|error| {
        ServiceError::Storage(format!("B2 authorize response was invalid: {error}"))
    })?;
    Ok(body.api_info.storage_api.s3_api_url)
}

fn region_from_endpoint(endpoint_url: &str) -> Result<String> {
    let parsed = Url::parse(endpoint_url).map_err(|error| {
        ServiceError::Configuration(format!("invalid B2 S3 endpoint URL: {error}"))
    })?;
    let host = parsed.host_str().ok_or_else(|| {
        ServiceError::Configuration("B2 S3 endpoint URL is missing a host".to_string())
    })?;
    if let Some(region) = host
        .strip_prefix("s3.")
        .and_then(|value| value.strip_suffix(".backblazeb2.com"))
    {
        return Ok(region.to_string());
    }
    Err(ServiceError::Configuration(format!(
        "cannot infer B2 region from S3 endpoint host {host:?}; set B2_S3_REGION"
    )))
}

fn validate_object_key(key: &str) -> Result<()> {
    let clean_key = key.trim_start_matches('/');
    if clean_key.is_empty()
        || clean_key.contains('\\')
        || clean_key.split('/').any(|part| part == "..")
    {
        return Err(ServiceError::InvalidObjectKey);
    }
    Ok(())
}

fn map_s3_error(error: impl std::fmt::Debug + std::fmt::Display) -> ServiceError {
    let display = error.to_string();
    let debug = format!("{error:?}");
    let message = if display == "service error" {
        debug
    } else {
        format!("{display}: {debug}")
    };
    if message.contains("PreconditionFailed")
        || message.contains("Precondition Failed")
        || message.contains("status code: 412")
        || message.contains(" 412")
    {
        return ServiceError::ObjectAlreadyExists;
    }
    if message.contains("NoSuchKey")
        || message.contains("NotFound")
        || message.contains("Not Found")
        || message.contains("status code: 404")
        || message.contains(" 404")
    {
        return ServiceError::ObjectNotFound;
    }
    ServiceError::Storage(message)
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name)
        .map(|value| value.trim().to_string())
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ServiceError::Configuration(format!("{name} is required")))
}

fn optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthorizeAccountResponse {
    api_info: ApiInfo,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiInfo {
    storage_api: StorageApiInfo,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StorageApiInfo {
    s3_api_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infers_region_from_b2_endpoint() {
        assert_eq!(
            region_from_endpoint("https://s3.us-west-004.backblazeb2.com").expect("region"),
            "us-west-004"
        );
    }

    #[test]
    fn validates_object_keys() {
        assert!(validate_object_key("a/b/c.json").is_ok());
        assert!(validate_object_key("").is_err());
        assert!(validate_object_key("../x").is_err());
        assert!(validate_object_key("a\\b").is_err());
    }
}
