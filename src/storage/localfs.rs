use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use walkdir::WalkDir;

use crate::error::{Result, ServiceError};
use crate::storage::{ListInput, ListPage, ListedObject, ObjectInfo, ObjectStore, PutOptions};

#[derive(Clone, Debug)]
pub struct LocalFileStore {
    root: PathBuf,
}

impl LocalFileStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        if root.as_os_str().is_empty() {
            return Err(ServiceError::InvalidObjectKey);
        }
        fs::create_dir_all(root)?;
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    fn path_for_key(&self, key: &str) -> Result<PathBuf> {
        let clean_key = key.trim_start_matches('/');
        if clean_key.is_empty() || clean_key.contains('\\') {
            return Err(ServiceError::InvalidObjectKey);
        }
        let relative = Path::new(clean_key);
        for component in relative.components() {
            match component {
                Component::Normal(_) => {}
                _ => return Err(ServiceError::InvalidObjectKey),
            }
        }
        Ok(self.root.join(relative))
    }
}

#[async_trait]
impl ObjectStore for LocalFileStore {
    async fn put(&self, key: &str, data: &[u8], options: PutOptions) -> Result<()> {
        let path = self.path_for_key(key)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut open_options = OpenOptions::new();
        open_options.write(true);
        if options.create_only {
            open_options.create_new(true);
        } else {
            open_options.create(true).truncate(true);
        }

        let mut file = open_options.open(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                ServiceError::ObjectAlreadyExists
            } else {
                ServiceError::Io(error)
            }
        })?;
        file.write_all(data)?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.path_for_key(key)?;
        fs::read(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ServiceError::ObjectNotFound
            } else {
                ServiceError::Io(error)
            }
        })
    }

    async fn head(&self, key: &str) -> Result<ObjectInfo> {
        let path = self.path_for_key(key)?;
        let metadata = fs::metadata(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ServiceError::ObjectNotFound
            } else {
                ServiceError::Io(error)
            }
        })?;
        Ok(ObjectInfo {
            key: key.to_string(),
            size: metadata.len(),
            content_type: String::new(),
            modified_at: DateTime::<Utc>::from(metadata.modified()?),
        })
    }

    async fn list(&self, mut input: ListInput) -> Result<ListPage> {
        if input.limit == 0 {
            input.limit = 100;
        }
        let prefix = input.prefix.trim_start_matches('/').to_string();
        let mut walk_root = self.root.clone();
        if !prefix.is_empty() {
            let mut prefix_directory = prefix.clone();
            if !prefix_directory.ends_with('/') {
                if let Some(last_slash) = prefix_directory.rfind('/') {
                    prefix_directory.truncate(last_slash + 1);
                } else {
                    prefix_directory.clear();
                }
            }
            if !prefix_directory.is_empty() {
                let candidate = self.path_for_key(&prefix_directory)?;
                if !candidate.exists() {
                    return Ok(ListPage::default());
                }
                walk_root = candidate;
            }
        }

        let mut objects = Vec::new();
        for entry in WalkDir::new(&walk_root) {
            let entry = entry.map_err(|error| ServiceError::Storage(error.to_string()))?;
            if !entry.file_type().is_file() {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(&self.root)
                .map_err(|error| ServiceError::Storage(error.to_string()))?;
            let key = relative
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            if !key.starts_with(&prefix) {
                continue;
            }
            if !input.start_after.is_empty() && key <= input.start_after {
                continue;
            }
            let metadata = entry
                .metadata()
                .map_err(|error| ServiceError::Io(error.into()))?;
            objects.push(ListedObject {
                key,
                size: metadata.len(),
                modified_at: DateTime::<Utc>::from(metadata.modified()?),
            });
        }

        objects.sort_by(|left, right| left.key.cmp(&right.key));
        let has_more = objects.len() > input.limit;
        if has_more {
            objects.truncate(input.limit);
        }
        let next_after_key = objects
            .last()
            .map(|object| object.key.clone())
            .unwrap_or_default();

        Ok(ListPage {
            objects,
            has_more,
            next_after_key,
        })
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.path_for_key(key)?;
        fs::remove_file(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ServiceError::ObjectNotFound
            } else {
                ServiceError::Io(error)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_get_list_and_create_only() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store = LocalFileStore::new(temp_dir.path()).expect("store");

        store
            .put(
                "a/b/one.json",
                br#"{"one":1}"#,
                PutOptions {
                    create_only: true,
                    content_type: String::new(),
                },
            )
            .await
            .expect("put one");
        store
            .put(
                "a/b/two.json",
                br#"{"two":2}"#,
                PutOptions {
                    create_only: true,
                    content_type: String::new(),
                },
            )
            .await
            .expect("put two");
        let conflict = store
            .put(
                "a/b/one.json",
                b"{}",
                PutOptions {
                    create_only: true,
                    content_type: String::new(),
                },
            )
            .await
            .expect_err("conflict");
        assert!(matches!(conflict, ServiceError::ObjectAlreadyExists));

        let data = store.get("a/b/one.json").await.expect("get");
        assert_eq!(data, br#"{"one":1}"#);

        let page = store
            .list(ListInput {
                prefix: "a/b/".to_string(),
                start_after: String::new(),
                limit: 1,
            })
            .await
            .expect("list");
        assert_eq!(page.objects.len(), 1);
        assert!(page.has_more);
        assert!(!page.next_after_key.is_empty());

        let next_page = store
            .list(ListInput {
                prefix: "a/b/".to_string(),
                start_after: page.next_after_key,
                limit: 10,
            })
            .await
            .expect("list next");
        assert_eq!(next_page.objects.len(), 1);
        assert_eq!(next_page.objects[0].key, "a/b/two.json");
    }
}
