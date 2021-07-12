/// A file based storage engine.
/// Each object is stored in 2 files:
/// ${object.id}.meta for the metadata serialized as Json.
/// ${object.id}.content for the opaque content.
use crate::common::{
    BoxedReader, ObjectId, ObjectKind, ObjectMetadata, ObjectStore, ObjectStoreError,
};
use async_std::{fs, fs::File, io::prelude::WriteExt, path::Path};
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
        content: Option<BoxedReader>,
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

        // 3. Store the content for leaf nodes.
        if let Some(content) = content {
            if metadata.kind() == ObjectKind::Leaf {
                let mut file = File::create(&content_path).await?;
                file.set_len(metadata.size() as _).await?;
                futures::io::copy(content, &mut file).await?;
                file.sync_all().await?;
            }
        }

        Ok(())
    }

    async fn update(
        &self,
        metadata: &ObjectMetadata,
        content: Option<BoxedReader>,
    ) -> Result<(), ObjectStoreError> {
        self.delete(metadata.id()).await?;
        self.create(metadata, content).await?;
        Ok(())
    }

    async fn update_content_from_slice(
        &self,
        id: ObjectId,
        content: &[u8],
    ) -> Result<(), ObjectStoreError> {
        let (_, content_path) = self.get_paths(id);
        let mut file = File::create(&content_path).await?;
        futures::io::copy(content, &mut file).await?;
        file.sync_all().await?;

        Ok(())
    }

    async fn delete(&self, id: ObjectId) -> Result<(), ObjectStoreError> {
        let (meta_path, content_path) = self.get_paths(id);
        fs::remove_file(&meta_path).await?;
        if Path::new(&content_path).exists().await {
            fs::remove_file(&content_path).await?;
        }
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
    ) -> Result<(ObjectMetadata, BoxedReader), ObjectStoreError> {
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

    async fn get_content(&self, id: ObjectId) -> Result<BoxedReader, ObjectStoreError> {
        let (_, content_path) = self.get_paths(id);

        let file = File::open(&content_path)
            .await
            .map_err(|_| ObjectStoreError::NoSuchObject)?;

        Ok(Box::new(file))
    }
}
