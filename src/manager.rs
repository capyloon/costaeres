/// Main structure, managing the overall flow of operations.
/// It uses a SQL database to store the metadata, and relies
/// on another store to provide the content.
///
/// When a request fails at the database level (eg. unknown object id),
/// it tries to re-hydrade the database by fetching the metadata
/// from the store.
///
/// The manager knows about Container and Leafs, and will do the appropriate
/// operations to maintain this data properly. That allows to get all the tree
/// structure without hitting the remote store.
///
/// In order to re-hydrate properly, a Container content is made of the list of
/// its children's id.
///
/// Any failure of the remote side leads to a rollback of the database transaction
/// to preserve the consistency between both sides.
use crate::common::{
    BoxedReader, ResourceId, ResourceKind, ResourceMetadata, ResourceStore, ResourceStoreError,
    TransactionResult, Variant, VariantContent, ROOT_ID,
};
use crate::config::Config;
use crate::fts::Fts;
use crate::indexer::Indexer;
use async_std::fs::File;
use bincode::Options;
use chrono::{DateTime, Utc};
use log::{debug, error};
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::collections::HashSet;

pub struct Manager {
    db_pool: SqlitePool,
    store: Box<dyn ResourceStore + Send + Sync>,
    fts: Fts,
    indexers: Vec<Box<dyn Indexer + Send + Sync>>, // The list of indexers available.
}

impl Manager {
    pub async fn new(
        config: Config,
        store: Box<dyn ResourceStore + Send + Sync>,
    ) -> Result<Self, ResourceStoreError> {
        // Create db file if needed.
        if let Err(_) = File::open(&config.db_path).await {
            let _file = File::create(&config.db_path).await?;
        }

        let db_pool = SqlitePool::connect(&format!("sqlite://{}", config.db_path)).await?;
        sqlx::migrate!("db/migrations")
            .run(&db_pool)
            .await
            .map_err(|err| {
                ResourceStoreError::Custom(format!("Failed to run migration: {}", err))
            })?;

        let fts = Fts::new(&db_pool, 5);
        Ok(Manager {
            db_pool,
            store,
            fts,
            indexers: Vec::new(),
        })
    }

    /// Use a existing transation to run the sql commands needed to create a metadata record.
    async fn create_metadata<'c>(
        &self,
        metadata: &ResourceMetadata,
        mut tx: Transaction<'c, Sqlite>,
    ) -> TransactionResult<'c> {
        let id = metadata.id();
        let parent = metadata.parent();
        let kind = metadata.kind();
        let name = metadata.name();
        let family = metadata.family();
        let size = metadata.size() as i64;
        let created = metadata.created();
        let modified = metadata.modified();
        let scorer = metadata.db_scorer();
        let frecency = metadata.scorer().frecency();
        sqlx::query!(
            r#"
    INSERT INTO resources ( id, parent, kind, name, family, size, created, modified, scorer, frecency )
    VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
            "#,
            id,
            parent,
            kind,
            name,
            family,
            size,
            created,
            modified,
            scorer,
            frecency,
        )
        .execute(&mut tx)
        .await?;

        // Insert the tags.
        for tag in metadata.tags() {
            sqlx::query!("INSERT INTO tags ( id, tag ) VALUES ( ?1, ?2 )", id, tag)
                .execute(&mut tx)
                .await?;
        }

        // Insert variants
        for variant in metadata.variants() {
            let name = variant.name();
            let mime_type = variant.mime_type();
            let size = variant.size();
            sqlx::query!(
                "INSERT INTO variants ( id, name, mimeType, size ) VALUES ( ?1, ?2, ?3, ?4 )",
                id,
                name,
                mime_type,
                size
            )
            .execute(&mut tx)
            .await?;
        }

        // Insert the full text search data.
        let tx2 = self.fts.add_text(id, &name, tx).await?;

        Ok(tx2)
    }

    /// Returns `true` if this object id is in the local index.
    pub async fn has_object(&self, id: ResourceId) -> Result<bool, ResourceStoreError> {
        let count = sqlx::query_scalar!("SELECT count(*) FROM resources WHERE id = ?", id)
            .fetch_one(&self.db_pool)
            .await?;

        Ok(count == 1)
    }

    /// Returns the number of resources in the local index.
    pub async fn resource_count(&self) -> Result<i32, ResourceStoreError> {
        let count = sqlx::query_scalar!("SELECT count(*) FROM resources")
            .fetch_one(&self.db_pool)
            .await?;

        Ok(count)
    }

    /// Returns `true` if this object id is in the local index and is a container.
    pub async fn is_container(&self, id: ResourceId) -> Result<bool, ResourceStoreError> {
        let count = sqlx::query_scalar!(
            "SELECT count(*) FROM resources WHERE id = ? and kind = ?",
            id,
            ResourceKind::Container
        )
        .fetch_one(&self.db_pool)
        .await?;

        Ok(count == 1)
    }

    /// Check container <-> leaf constraints
    // container == leaf is only valid for the root (container == 0)
    pub async fn check_container_leaf(
        &self,
        id: ResourceId,
        parent: ResourceId,
    ) -> Result<(), ResourceStoreError> {
        if parent == id && parent != ROOT_ID {
            error!("Only the root can be its own container.");
            return Err(ResourceStoreError::InvalidContainerId);
        }
        // Check that the parent is a known container, except when we create the root.
        if id != ROOT_ID && !self.is_container(parent).await? {
            error!("Resource #{} is not a container", parent);
            return Err(ResourceStoreError::InvalidContainerId);
        }

        Ok(())
    }

    pub async fn children_of<'c, E: sqlx::Executor<'c, Database = Sqlite>>(
        &self,
        parent: ResourceId,
        executor: E,
    ) -> Result<Vec<ResourceId>, ResourceStoreError> {
        let children: Vec<ResourceId> = sqlx::query!(
            "SELECT id FROM resources WHERE parent = ? AND parent != id",
            parent
        )
        .fetch_all(executor)
        .await?
        .iter()
        .map(|r| r.id.into())
        .collect();

        Ok(children)
    }

    pub async fn serialize_children_of<'c, E: sqlx::Executor<'c, Database = Sqlite>>(
        &self,
        parent: ResourceId,
        executor: E,
    ) -> Result<Vec<u8>, ResourceStoreError> {
        let children = self.children_of(parent, executor).await?;
        let bincode = bincode::options().with_big_endian().with_varint_encoding();
        let res = bincode.serialize(&children)?;

        Ok(res)
    }

    pub async fn update_container_content<'c, E: sqlx::Executor<'c, Database = Sqlite>>(
        &self,
        parent: ResourceId,
        executor: E,
    ) -> Result<(), ResourceStoreError> {
        let children = self.serialize_children_of(parent, executor).await?;
        self.store
            .update_default_variant_from_slice(parent, &children)
            .await?;

        Ok(())
    }

    pub async fn parent_of<'c, E: sqlx::Executor<'c, Database = Sqlite>>(
        &self,
        id: ResourceId,
        executor: E,
    ) -> Result<ResourceId, ResourceStoreError> {
        let maybe_parent = sqlx::query!("SELECT parent FROM resources WHERE id = ?", id)
            .fetch_optional(executor)
            .await?;

        if let Some(record) = maybe_parent {
            return Ok(record.parent.into());
        }
        Err(ResourceStoreError::NoSuchResource)
    }

    pub async fn clear(&self) -> Result<(), ResourceStoreError> {
        let mut tx = self.db_pool.begin().await?;
        sqlx::query!("DELETE FROM resources")
            .execute(&mut tx)
            .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn create_root(&self) -> Result<(), ResourceStoreError> {
        let root = ResourceMetadata::new(
            0.into(),
            0.into(),
            ResourceKind::Container,
            0,
            "/",
            "<root>",
            vec![],
            vec![Variant::new("default", "inode/directory", 0)],
        );
        self.create(&root, None).await
    }

    pub async fn get_root(
        &self,
    ) -> Result<(ResourceMetadata, Vec<ResourceMetadata>), ResourceStoreError> {
        self.get_container(ROOT_ID).await
    }

    // Returns the whole set of object metadata from the root to the given object.
    // Will fail if a cycle is detected or if any parent id fails to return metadata.
    pub async fn get_full_path(
        &self,
        id: ResourceId,
    ) -> Result<Vec<ResourceMetadata>, ResourceStoreError> {
        let mut res = vec![];
        let mut current = id;
        let mut visited = HashSet::new();

        loop {
            if visited.contains(&current) {
                return Err(ResourceStoreError::ResourceCycle);
            }
            let meta = self.get_metadata(current).await?;
            visited.insert(current);
            let next = meta.parent();
            res.push(meta);
            if current == ROOT_ID {
                break;
            }
            current = next;
        }

        // Make sure we order elements from root -> target node.
        res.reverse();
        Ok(res)
    }

    // Retrieve the list of objects matching the given name, optionnaly restricted to a given mime type.
    // TODO: pagination
    pub async fn by_name(
        &self,
        name: &str,
        mime: Option<&str>,
    ) -> Result<Vec<ResourceId>, ResourceStoreError> {
        if name.trim().is_empty() {
            return Err(ResourceStoreError::Custom("EmptyNameQuery".into()));
        }

        let results: Vec<ResourceId> = if let Some(mime) = mime {
            sqlx::query!(
                "SELECT id FROM resources WHERE name = ? and family = ? ORDER BY frecency DESC",
                name,
                mime
            )
            .fetch_all(&self.db_pool)
            .await?
            .iter()
            .map(|r| r.id.into())
            .collect()
        } else {
            sqlx::query!(
                "SELECT id FROM resources WHERE name = ? ORDER BY frecency DESC",
                name,
            )
            .fetch_all(&self.db_pool)
            .await?
            .iter()
            .map(|r| r.id.into())
            .collect()
        };

        Ok(results)
    }

    // Retrieve the object with a given name and parent.
    pub async fn child_by_name(
        &self,
        parent: ResourceId,
        name: &str,
    ) -> Result<ResourceMetadata, ResourceStoreError> {
        if name.trim().is_empty() {
            return Err(ResourceStoreError::Custom("EmptyNameQuery".into()));
        }

        let record = sqlx::query!(
            "SELECT id FROM resources WHERE parent = ? AND name = ?",
            parent,
            name,
        )
        .fetch_optional(&self.db_pool)
        .await?;

        match record {
            Some(child) => self.get_metadata(child.id.into()).await,
            None => Err(ResourceStoreError::NoSuchResource),
        }
    }

    // Retrieve the list of objects matching the given tag, optionnaly restricted to a given mime type.
    // TODO: pagination
    pub async fn by_tag(
        &self,
        tag: &str,
        mime: Option<&str>,
    ) -> Result<Vec<ResourceId>, ResourceStoreError> {
        if tag.trim().is_empty() {
            return Err(ResourceStoreError::Custom("EmptyTagQuery".into()));
        }

        let results: Vec<ResourceId> = if let Some(mime) = mime {
            sqlx::query!(
                r#"SELECT resources.id FROM resources
                   LEFT JOIN tags
                   WHERE tags.tag = ? and tags.id = resources.id and resources.family = ?
                   ORDER BY frecency DESC"#,
                tag,
                mime
            )
            .fetch_all(&self.db_pool)
            .await?
            .iter()
            .map(|r| r.id.into())
            .collect()
        } else {
            sqlx::query!(
                r#"SELECT resources.id FROM resources
            LEFT JOIN tags
            WHERE tags.tag = ? and tags.id = resources.id
            ORDER BY frecency DESC"#,
                tag
            )
            .fetch_all(&self.db_pool)
            .await?
            .iter()
            .map(|r| r.id.into())
            .collect()
        };

        Ok(results)
    }

    pub async fn by_text(
        &self,
        text: &str,
        family: Option<String>,
    ) -> Result<Vec<(ResourceId, u32)>, ResourceStoreError> {
        if text.trim().is_empty() {
            return Err(ResourceStoreError::Custom("EmptyTextQuery".into()));
        }

        self.fts.search(text, family).await
    }

    pub async fn top_by_frecency(
        &self,
        count: u32,
    ) -> Result<Vec<(ResourceId, u32)>, ResourceStoreError> {
        if count == 0 {
            return Err(ResourceStoreError::Custom("ZeroCountQuery".into()));
        }

        let results: Vec<(ResourceId, u32)> = sqlx::query!(
            "SELECT id, frecency FROM resources ORDER BY frecency DESC LIMIT ?",
            count,
        )
        .fetch_all(&self.db_pool)
        .await?
        .iter()
        .map(|r| (r.id.into(), r.frecency as u32))
        .collect();

        Ok(results)
    }

    pub async fn update_text_index<'c>(
        &'c self,
        metadata: &'c ResourceMetadata,
        content: &mut BoxedReader,
        mut tx: Transaction<'c, Sqlite>,
    ) -> TransactionResult<'c> {
        if metadata.kind() == ResourceKind::Container {
            return Ok(tx);
        }

        for indexer in &self.indexers {
            tx = indexer.index(&metadata, content, &self.fts, tx).await?
        }

        Ok(tx)
    }

    pub fn add_indexer(&mut self, indexer: Box<dyn Indexer + Send + Sync>) {
        self.indexers.push(indexer);
    }

    pub async fn close(&self) {
        self.db_pool.close().await
    }

    pub async fn next_id(&self) -> Result<ResourceId, ResourceStoreError> {
        let max = sqlx::query_scalar!("SELECT id FROM resources ORDER BY id DESC LIMIT 1")
            .fetch_one(&self.db_pool)
            .await?;

        Ok((max + 1).into())
    }

    pub async fn create(
        &self,
        metadata: &ResourceMetadata,
        mut content: Option<VariantContent>,
    ) -> Result<(), ResourceStoreError> {
        self.check_container_leaf(metadata.id(), metadata.parent())
            .await?;

        // Start a transaction to store the new metadata.
        let tx = self.db_pool.begin().await?;
        let mut tx2 = self.create_metadata(metadata, tx).await?;

        // Update the children content of the parent if this is not creating the root.
        if metadata.id() != ROOT_ID {
            self.update_container_content(metadata.parent(), &mut tx2)
                .await?;
        }

        // If there is content run the text indexer for this mime type.
        let tx3 = if let Some(ref mut content) = content {
            self.update_text_index(&metadata, &mut content.1, tx2)
                .await?
        } else {
            tx2
        };

        // Create the store entry, and commit the SQlite transaction in case of success.
        match self.store.create(metadata, content).await {
            Ok(_) => {
                tx3.commit().await?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    pub async fn update(
        &self,
        metadata: &ResourceMetadata,
        mut content: Option<VariantContent>,
    ) -> Result<(), ResourceStoreError> {
        self.check_container_leaf(metadata.id(), metadata.parent())
            .await?;

        let mut tx = self.db_pool.begin().await?;
        let id = metadata.id();
        sqlx::query!("DELETE FROM resources WHERE id = ?", id)
            .execute(&mut tx)
            .await?;

        let mut tx2 = self.create_metadata(metadata, tx).await?;

        // Update the children content of the parent if this is not creating the root.
        if metadata.id() != ROOT_ID {
            self.update_container_content(metadata.parent(), &mut tx2)
                .await?;
        }

        // If there is content, run the text indexer for this mime type.
        let tx3 = if let Some(ref mut content) = content {
            self.update_text_index(&metadata, &mut content.1, tx2)
                .await?
        } else {
            tx2
        };

        match self.store.update(metadata, content).await {
            Ok(_) => {
                tx3.commit().await?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    pub async fn delete_variant(
        &self,
        id: ResourceId,
        variant_name: &str,
    ) -> Result<(), ResourceStoreError> {
        // 1. Get the metadata for this id.
        let mut metadata = self.get_metadata(id).await?;

        // 2. Check variant validity
        if !metadata.has_variant(variant_name) {
            error!("Variant '{}' is not in metadata.", variant_name);
            return Err(ResourceStoreError::InvalidVariant(variant_name.into()));
        }

        // 3. remove variant from database and store
        sqlx::query!(
            "DELETE FROM variants WHERE id = ? AND name = ?",
            id,
            variant_name
        )
        .execute(&self.db_pool)
        .await?;
        metadata.delete_variant(variant_name);
        self.store.delete_variant(id, variant_name).await?;

        // 4. Perform an update with no variant to keep the metadata up to date.
        self.store.update(&metadata, None).await?;

        Ok(())
    }

    pub async fn delete(&self, id: ResourceId) -> Result<(), ResourceStoreError> {
        let mut tx = self.db_pool.begin().await?;
        let is_container = self.is_container(id).await?;

        let parent_id = self.parent_of(id, &mut tx).await?;

        // Delete the object itself.
        // The tags will be removed by the delete cascade sql rule.
        sqlx::query!("DELETE FROM resources WHERE id = ?", id)
            .execute(&mut tx)
            .await?;

        if !is_container {
            self.store.delete(id).await?;
            self.update_container_content(parent_id, &mut tx).await?;
            tx.commit().await?;
            return Ok(());
        }

        // Collect all the children, in a non-recursive way.

        // This set holds the list of all children to remove.
        let mut to_delete: HashSet<ResourceId> = HashSet::new();

        // This vector holds the list of remaining containers
        // that need to be checked.
        let mut containers: Vec<ResourceId> = vec![id];

        loop {
            let mut new_obj = vec![];

            for source_id in containers {
                let children: Vec<ResourceId> = self.children_of(source_id, &self.db_pool).await?;

                for child in children {
                    // 1. add this child to the final set.
                    to_delete.insert(child);
                    // 2. If it's a container, add it to the list of containers for the next iteration.
                    if self.is_container(child).await? {
                        new_obj.push(child);
                    }
                }
            }

            if new_obj.is_empty() {
                break;
            }

            // swap the containers to iterate over in the next loop iteration.
            containers = new_obj;
        }

        for child in to_delete {
            // Delete the child.
            // The tags will be removed by the delete cascade sql rule.
            sqlx::query!("DELETE FROM resources WHERE id = ?", child)
                .execute(&mut tx)
                .await?;
            self.store.delete(child).await?;
        }

        self.store.delete(id).await?;
        self.update_container_content(parent_id, &mut tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_metadata(
        &self,
        id: ResourceId,
    ) -> Result<ResourceMetadata, ResourceStoreError> {
        // Metadata can be retrieved fully from the SQL database.
        match sqlx::query!(
            r#"
    SELECT id, parent, kind, name, family, size, created, modified, scorer FROM resources
    WHERE id = ?"#,
            id
        )
        .fetch_one(&self.db_pool)
        .await
        {
            Ok(record) => {
                let mut meta = ResourceMetadata::new(
                    record.id.into(),
                    record.parent.into(),
                    record.kind.into(),
                    record.size,
                    &record.name,
                    &record.family,
                    vec![],
                    vec![],
                );

                // Get the tags if any.
                let tags: Vec<String> = sqlx::query!("SELECT tag FROM tags WHERE id = ?", id)
                    .fetch_all(&self.db_pool)
                    .await?
                    .iter()
                    .map(|r| r.tag.clone())
                    .collect();

                if !tags.is_empty() {
                    meta.set_tags(tags);
                }

                // Get the variants if any.
                let variants: Vec<Variant> =
                    sqlx::query!("SELECT name, mimeType, size FROM variants WHERE id = ?", id)
                        .fetch_all(&self.db_pool)
                        .await?
                        .iter()
                        .map(|r| Variant::new(&r.name, &r.mimeType, r.size as _))
                        .collect();

                if !variants.is_empty() {
                    meta.set_variants(variants);
                }

                meta.set_created(DateTime::<Utc>::from_utc(record.created, Utc));
                meta.set_modified(DateTime::<Utc>::from_utc(record.modified, Utc));
                meta.set_scorer_from_db(&record.scorer);
                Ok(meta)
            }
            Err(err) => {
                // Rehydrate from the object storage.
                debug!(
                    "Metadata for object #{} not in db ({}), fetching it from object storage.",
                    id, err
                );
                // Err(ResourceStoreError::NoSuchResource)
                let metadata = self.store.get_metadata(id).await?;
                let tx = self.db_pool.begin().await?;
                let tx2 = self.create_metadata(&metadata, tx).await?;
                tx2.commit().await?;

                Ok(metadata)
            }
        }
    }

    pub async fn get_leaf(
        &self,
        id: ResourceId,
        variant_name: &str,
    ) -> Result<(ResourceMetadata, BoxedReader), ResourceStoreError> {
        let meta = self.get_metadata(id).await?;

        if meta.kind() != ResourceKind::Leaf {
            return Err(ResourceStoreError::NoSuchResource);
        }

        // Just relay content from the underlying store since we don't keep the content in the index.
        Ok((meta, self.store.get_variant(id, variant_name).await?))
    }

    pub async fn get_container(
        &self,
        id: ResourceId,
    ) -> Result<(ResourceMetadata, Vec<ResourceMetadata>), ResourceStoreError> {
        use async_std::io::ReadExt;

        let meta = self.get_metadata(id).await?;

        if meta.kind() != ResourceKind::Container {
            return Err(ResourceStoreError::NoSuchResource);
        }

        // Read the list of children from the container content.
        if let Ok(mut file) = self.store.get_variant(id, "default").await {
            let mut buffer = vec![];
            file.read_to_end(&mut buffer).await?;
            let bincode = bincode::options().with_big_endian().with_varint_encoding();
            let children: Vec<ResourceId> = bincode.deserialize(&buffer)?;

            // Get the metadata for each child.
            let mut res = vec![];
            for child in children {
                res.push(self.get_metadata(child).await?);
            }

            Ok((meta, res))
        } else {
            // No children for this container.
            Ok((meta, vec![]))
        }
    }
}
