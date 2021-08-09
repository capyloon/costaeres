/// A file based storage engine.
/// Each object is stored in 2 files:
/// ${object.id}.meta for the metadata serialized as Json.
/// ${object.id}.content for the opaque content.
use crate::common::{
    BoxedReader, ResourceId, ResourceKind, ResourceMetadata, ResourceStore, ResourceStoreError,
    VariantContent,
};
use async_std::{
    fs,
    fs::File,
    io::prelude::WriteExt,
    path::{Path, PathBuf},
};
use async_trait::async_trait;
use log::error;

macro_rules! custom_error {
    ($error:expr) => {
        Err(ResourceStoreError::Custom($error.into()))
    };
}

#[derive(Clone)]
pub struct FileStore {
    root: PathBuf, // The root path of the storage.
}

impl FileStore {
    pub async fn new<P>(path: P) -> Result<Self, ResourceStoreError>
    where
        P: AsRef<Path>,
    {
        // Fail if the root is not an existing directory.
        let file = File::open(&path).await?;
        let meta = file.metadata().await?;
        if !meta.is_dir() {
            return custom_error!("NotDirectory");
        }
        let root = path.as_ref().to_path_buf();
        Ok(Self { root })
    }

    fn meta_path(&self, id: &ResourceId) -> PathBuf {
        let mut meta_path = self.root.clone();
        meta_path.push(&format!("{}.meta", id));
        meta_path
    }

    fn variant_path(&self, id: &ResourceId, variant: &str) -> PathBuf {
        let mut content_path = self.root.clone();
        content_path.push(&format!("{}.content.{}", id, variant));
        content_path
    }

    async fn create_or_update(
        &self,
        metadata: &ResourceMetadata,
        content: Option<VariantContent>,
        create: bool,
    ) -> Result<(), ResourceStoreError> {
        // 0. TODO: check if we have enough storage available.

        let id = metadata.id();
        let meta_path = self.meta_path(&id);

        // 1. When creating, check if we already have files for this id, and bail out if so.
        if create {
            let file = File::open(&meta_path).await;
            if file.is_ok() {
                error!("Can't create two files with path {}", meta_path.display());
                return Err(ResourceStoreError::ResourceAlreadyExists);
            }
        }

        // 2. Store the metadata.
        let mut file = File::create(&meta_path).await?;
        let meta = serde_json::to_vec(&metadata)?;
        file.write_all(&meta).await?;
        file.sync_all().await?;

        // 3. Store the variants for leaf nodes.
        if metadata.kind() != ResourceKind::Leaf {
            return Ok(());
        }

        if let Some(content) = content {
            let name = content.0.name();
            if !metadata.has_variant(&name) {
                error!("Variant '{}' is not in metadata.", name);
                return Err(ResourceStoreError::InvalidVariant(name));
            }
            let mut file = File::create(&self.variant_path(&id, &name)).await?;
            file.set_len(content.0.size() as _).await?;
            futures::io::copy(content.1, &mut file).await?;
            file.sync_all().await?;
        }

        Ok(())
    }
}

#[async_trait(?Send)]
impl ResourceStore for FileStore {
    async fn create(
        &self,
        metadata: &ResourceMetadata,
        content: Option<VariantContent>,
    ) -> Result<(), ResourceStoreError> {
        self.create_or_update(metadata, content, true).await
    }

    async fn update(
        &self,
        metadata: &ResourceMetadata,
        content: Option<VariantContent>,
    ) -> Result<(), ResourceStoreError> {
        self.create_or_update(metadata, content, false).await
    }

    async fn update_default_variant_from_slice(
        &self,
        id: &ResourceId,
        content: &[u8],
    ) -> Result<(), ResourceStoreError> {
        let content_path = self.variant_path(id, "default");
        let mut file = File::create(&content_path).await?;
        futures::io::copy(content, &mut file).await?;
        file.sync_all().await?;

        Ok(())
    }

    async fn delete(&self, id: &ResourceId) -> Result<(), ResourceStoreError> {
        // 1. get the metadata in order to know all the possible variants.
        let metadata = self.get_metadata(id).await?;

        // 2. remove the metadata.
        let meta_path = self.meta_path(id);
        fs::remove_file(&meta_path).await?;

        // 3. remove variants.
        for variant in metadata.variants() {
            let path = self.variant_path(id, &variant.name());
            if Path::new(&path).exists().await {
                fs::remove_file(&path).await?;
            }
        }
        Ok(())
    }

    async fn delete_variant(
        &self,
        id: &ResourceId,
        variant: &str,
    ) -> Result<(), ResourceStoreError> {
        let path = self.variant_path(id, variant);
        if Path::new(&path).exists().await {
            fs::remove_file(&path).await?;
        }
        Ok(())
    }

    async fn get_metadata(&self, id: &ResourceId) -> Result<ResourceMetadata, ResourceStoreError> {
        use async_std::io::ReadExt;

        let meta_path = self.meta_path(id);

        let mut file = File::open(&meta_path)
            .await
            .map_err(|_| ResourceStoreError::NoSuchResource)?;
        let mut buffer = vec![];
        file.read_to_end(&mut buffer).await?;
        let metadata: ResourceMetadata = serde_json::from_slice(&buffer)?;

        Ok(metadata)
    }

    async fn get_full(
        &self,
        id: &ResourceId,
        name: &str,
    ) -> Result<(ResourceMetadata, BoxedReader), ResourceStoreError> {
        use async_std::io::ReadExt;

        let meta_path = self.meta_path(id);

        let mut file = File::open(&meta_path)
            .await
            .map_err(|_| ResourceStoreError::NoSuchResource)?;
        let mut buffer = vec![];
        file.read_to_end(&mut buffer).await?;
        let metadata: ResourceMetadata = serde_json::from_slice(&buffer)?;

        let content_path = self.variant_path(id, name);
        let file = File::open(&content_path)
            .await
            .map_err(|_| ResourceStoreError::NoSuchResource)?;

        Ok((metadata, Box::new(file)))
    }

    async fn get_variant(
        &self,
        id: &ResourceId,
        name: &str,
    ) -> Result<BoxedReader, ResourceStoreError> {
        let content_path = self.variant_path(id, name);

        let file = File::open(&content_path)
            .await
            .map_err(|_| ResourceStoreError::NoSuchResource)?;

        Ok(Box::new(file))
    }
}
