use std::sync::Arc;

use s3_message_service::application::{Service, ServiceOptions};
use s3_message_service::config::Config;
use s3_message_service::error::Result;
use s3_message_service::httpapi;
use s3_message_service::ids::IdGenerator;
use s3_message_service::keys::KeyBuilder;
use s3_message_service::storage::ObjectStore;
use s3_message_service::storage::b2::{B2Config, B2ObjectStore};
use s3_message_service::storage::localfs::LocalFileStore;

#[tokio::main]
async fn main() -> Result<()> {
    let runtime_config = Config::load_from_env()?;
    let store: Arc<dyn ObjectStore> = match runtime_config.storage_provider.as_str() {
        "filesystem" => Arc::new(LocalFileStore::new(&runtime_config.filesystem_root)?),
        "b2" | "backblaze-b2" => Arc::new(B2ObjectStore::from_config(B2Config::from_env()?).await?),
        provider => {
            return Err(s3_message_service::error::ServiceError::Configuration(
                format!("unsupported storage provider {provider:?}"),
            ));
        }
    };
    let service = Arc::new(Service::new(ServiceOptions {
        store,
        key_builder: KeyBuilder::new(&runtime_config.object_namespace),
        id_generator: IdGenerator::new(),
        clock: None,
        max_page_size: runtime_config.max_page_size,
        read_lookback_minutes: runtime_config.read_lookback_minutes,
    }));

    let app = httpapi::router(service);
    let bind_address = normalize_bind_address(&runtime_config.http_address);
    let listener = tokio::net::TcpListener::bind(&bind_address).await?;
    println!(
        "s3-message-service listening on {}",
        runtime_config.http_address
    );
    axum::serve(listener, app).await?;
    Ok(())
}

fn normalize_bind_address(address: &str) -> String {
    if address.starts_with(':') {
        format!("0.0.0.0{address}")
    } else {
        address.to_string()
    }
}
