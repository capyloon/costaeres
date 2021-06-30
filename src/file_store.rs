/// A file based storage engine.
/// Each object is stored in 2 files:
/// ${object.id}.meta for the metadata serialized as Json.
/// ${object.id}.content for the opaque content.
use crate::common::{ObjectId, ObjectMetadata, ObjectStore, ObjectStoreError};
use async_std::{fs, fs::File, io::prelude::WriteExt, io::Read};
use async_trait::async_trait;
use std::path::PathBuf;

macro_rules! custom_error {
    ($error:expr) => {
        Err(ObjectStoreError::Custom($error.into()))
    };
}

pub struct FileStore {
    root: PathBuf, // The root path of the storage.
}

impl FileStore {
    pub async fn new(root: &str) -> Result<Self, ObjectStoreError> {
        // Fail if the root is not an existing directory.
        let file = File::open(root).await?;
        let meta = file.metadata().await?;
        if !meta.is_dir() {
            return custom_error!("NotDirectory");
        }
        Ok(Self {
            root: PathBuf::from(root),
        })
    }

    fn get_paths(&self, id: ObjectId) -> (PathBuf, PathBuf) {
        let mut meta_path = self.root.clone();
        meta_path.push(&format!("{}.meta", id));
        let mut content_path = self.root.clone();
        content_path.push(&format!("{}.content", id));
        (meta_path, content_path)
    }
}

#[async_trait(?Send)]
impl ObjectStore for FileStore {
    async fn create(
        &self,
        metadata: &ObjectMetadata,
        content: Box<dyn Read + Unpin>,
    ) -> Result<(), ObjectStoreError> {
        // 0. TODO: check if we have enough storage available.

        let (meta_path, content_path) = self.get_paths(metadata.id());

        // 1. Check if we already have files for this id, and bail out if so.
        let file = File::open(&meta_path).await;
        if file.is_ok() {
            return Err(ObjectStoreError::ObjectAlreadyExists);
        }

        // 2. Store the metadata.
        let mut file = File::create(&meta_path).await?;
        let meta = serde_json::to_vec(&metadata)?;
        file.write_all(&meta).await?;
        file.sync_all().await?;

        // 3. Store the content
        let mut file = File::create(&content_path).await?;
        file.set_len(metadata.size() as _).await?;
        futures::io::copy(content, &mut file).await?;
        file.sync_all().await?;

        Ok(())
    }

    async fn update(
        &self,
        metadata: &ObjectMetadata,
        content: Box<dyn Read + Unpin>,
    ) -> Result<(), ObjectStoreError> {
        self.delete(metadata.id()).await?;
        self.create(metadata, content).await?;
        Ok(())
    }

    async fn delete(&self, id: ObjectId) -> Result<(), ObjectStoreError> {
        let (meta_path, content_path) = self.get_paths(id);
        fs::remove_file(&meta_path).await?;
        fs::remove_file(&content_path).await?;

        Ok(())
    }

    async fn get_metadata(&self, id: ObjectId) -> Result<ObjectMetadata, ObjectStoreError> {
        use async_std::io::ReadExt;

        let (meta_path, _) = self.get_paths(id);

        let mut file = File::open(&meta_path)
            .await
            .map_err(|_| ObjectStoreError::NoSuchObject)?;
        let mut buffer = vec![];
        file.read_to_end(&mut buffer).await?;
        let metadata: ObjectMetadata = serde_json::from_slice(&buffer)?;

        Ok(metadata)
    }

    async fn get_full(
        &self,
        id: ObjectId,
    ) -> Result<(ObjectMetadata, Box<dyn Read>), ObjectStoreError> {
        use async_std::io::ReadExt;

        let (meta_path, content_path) = self.get_paths(id);

        let mut file = File::open(&meta_path)
            .await
            .map_err(|_| ObjectStoreError::NoSuchObject)?;
        let mut buffer = vec![];
        file.read_to_end(&mut buffer).await?;
        let metadata: ObjectMetadata = serde_json::from_slice(&buffer)?;

        let file = File::open(&content_path)
            .await
            .map_err(|_| ObjectStoreError::NoSuchObject)?;

        Ok((metadata, Box::new(file)))
    }
}

#[cfg(test)]
mod tests {
    static CONTENT: [u8; 100] = [0; 100];

    use super::*;

    #[async_std::test]
    async fn file_store() {
        use crate::common::ObjectKind;
        use async_std::fs;

        let _ = fs::remove_dir_all("./test-content/0").await;
        let _ = fs::create_dir_all("./test-content/0").await;

        let store = FileStore::new("./test-content/0").await.unwrap();

        // Starting with no content.
        let res = store.get_full(0.into()).await.err();
        assert_eq!(res, Some(ObjectStoreError::NoSuchObject));

        // Adding an object.
        let meta = ObjectMetadata::new(
            0.into(),
            0.into(),
            ObjectKind::Leaf,
            10,
            "object 0",
            "text/plain",
            Some(vec!["one".into(), "two".into()]),
        );

        let res = store.create(&meta, Box::new(&CONTENT[..])).await.ok();
        assert_eq!(res, Some(()));

        // Now check that we can get it.
        let res = store.get_full(0.into()).await.ok().unwrap().0;
        assert_eq!(res.id(), 0.into());
        assert_eq!(&res.name(), "object 0");

        // Check we can't add another object with the same id.
        let res = store.create(&meta, Box::new(&CONTENT[..])).await.err();
        assert_eq!(res, Some(ObjectStoreError::ObjectAlreadyExists));

        // Update the object.
        let meta = ObjectMetadata::new(
            0.into(),
            0.into(),
            ObjectKind::Leaf,
            10,
            "object 0 updated",
            "text/plain",
            Some(vec!["one".into(), "two".into()]),
        );

        let _ = store.update(&meta, Box::new(&CONTENT[..])).await.unwrap();

        let res = store.get_full(0.into()).await.ok().unwrap().0;
        assert_eq!(res.id(), 0.into());
        assert_eq!(&res.name(), "object 0 updated");

        // Now delete this object.
        let _ = store.delete(0.into()).await.ok().unwrap();

        // And check we can't get it anymore.
        let res = store.get_full(0.into()).await.err();
        assert_eq!(res, Some(ObjectStoreError::NoSuchObject));
    }
}
