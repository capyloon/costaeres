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
    BoxedReader, IdFrec, ResourceId, ResourceKind, ResourceMetadata, ResourceStore,
    ResourceStoreError, TransactionResult, Variant, VariantContent, ROOT_ID,
};
use crate::config::Config;
use crate::fts::Fts;
use crate::indexer::Indexer;
use crate::scorer::sqlite_frecency;
use crate::scorer::VisitEntry;
use crate::timer::Timer;
use bincode::Options;
use chrono::{DateTime, Utc};
use libsqlite3_sys::{
    sqlite3_create_function, SQLITE_DETERMINISTIC, SQLITE_DIRECTONLY, SQLITE_INNOCUOUS, SQLITE_UTF8,
};
use log::{debug, error};
use lru::LruCache;
use sqlx::ConnectOptions;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Sqlite, SqlitePool, Transaction,
};
use std::collections::HashSet;
use std::ffi::CString;
use std::str::FromStr;

pub struct Manager {
    db_pool: SqlitePool,
    store: Box<dyn ResourceStore + Send + Sync>,
    fts: Fts,
    indexers: Vec<Box<dyn Indexer + Send + Sync>>, // The list of indexers available.
    cache: LruCache<ResourceId, ResourceMetadata>, // Cache frequently accessed metadata.
}

impl Manager {
    pub async fn new(
        config: Config,
        store: Box<dyn ResourceStore + Send + Sync>,
    ) -> Result<Self, ResourceStoreError> {
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", config.db_path))?
            .create_if_missing(true)
            .auto_vacuum(sqlx::sqlite::SqliteAutoVacuum::Incremental)
            .log_statements(log::LevelFilter::Trace)
            .log_slow_statements(
                log::LevelFilter::Error,
                std::time::Duration::from_millis(100),
            )
            .clone();

        // Register our custom function to evaluate frecency based on the scorer serialized representation.
        let pool_options = SqlitePoolOptions::new().after_connect(|conn| {
            Box::pin(async move {
                let handle = conn.as_raw_handle();

                let name = CString::new("frecency").unwrap();
                unsafe {
                    sqlite3_create_function(
                        handle,
                        name.as_ptr(),
                        1, // Argument count.
                        SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS | SQLITE_DIRECTONLY,
                        std::ptr::null_mut(),
                        Some(sqlite_frecency),
                        None,
                        None,
                    );
                }
                Ok(())
            })
        });

        let db_pool = pool_options.connect_with(options).await?;
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
            cache: LruCache::new(config.metadata_cache_capacity),
        })
    }

    fn evict_from_cache(&mut self, id: &ResourceId) {
        self.cache.pop(id);
    }

    fn update_cache(&mut self, metadata: &ResourceMetadata) {
        self.cache.put(metadata.id(), (*metadata).clone());
    }

    /// Update the frecency for that metadata.
    pub async fn visit(
        &mut self,
        metadata: &mut ResourceMetadata,
        visit: &VisitEntry,
    ) -> Result<(), ResourceStoreError> {
        metadata.update_scorer(visit);

        let id = metadata.id();
        let scorer = metadata.db_scorer();
        // We only need to update the scorer, so not doing a full update here.
        sqlx::query!(
            "UPDATE OR REPLACE resources SET scorer = ? WHERE id = ?",
            scorer,
            id
        )
        .execute(&self.db_pool)
        .await?;

        self.evict_from_cache(&id);

        Ok(())
    }

    /// Use a existing transation to run the sql commands needed to create a metadata record.
    async fn create_metadata<'c>(
        &mut self,
        metadata: &ResourceMetadata,
        mut tx: Transaction<'c, Sqlite>,
    ) -> TransactionResult<'c> {
        let _timer = Timer::start("create_metadata");
        let id = metadata.id();
        let parent = metadata.parent();
        let kind = metadata.kind();
        let name = metadata.name();
        let created = metadata.created();
        let modified = metadata.modified();
        let scorer = metadata.db_scorer();
        sqlx::query!(
            r#"
    INSERT INTO resources ( id, parent, kind, name, created, modified, scorer )
    VALUES ( ?, ?, ?, ?, ?, ?, ? )
            "#,
            id,
            parent,
            kind,
            name,
            created,
            modified,
            scorer,
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

        self.update_cache(metadata);

        Ok(tx2)
    }

    /// Returns `true` if this object id is in the local index.
    pub async fn has_object(&self, id: &ResourceId) -> Result<bool, ResourceStoreError> {
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
    pub async fn is_container(&self, id: &ResourceId) -> Result<bool, ResourceStoreError> {
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
        id: &ResourceId,
        parent: &ResourceId,
    ) -> Result<(), ResourceStoreError> {
        if parent == id && !parent.is_root() {
            error!("Only the root can be its own container.");
            return Err(ResourceStoreError::InvalidContainerId);
        }
        // Check that the parent is a known container, except when we create the root.
        if !id.is_root() && !self.is_container(parent).await? {
            error!("Resource #{} is not a container", parent);
            return Err(ResourceStoreError::InvalidContainerId);
        }

        Ok(())
    }

    pub async fn children_of<'c, E: sqlx::Executor<'c, Database = Sqlite>>(
        &self,
        parent: &ResourceId,
        executor: E,
    ) -> Result<Vec<ResourceId>, ResourceStoreError> {
        let children: Vec<ResourceId> = sqlx::query!(
            "SELECT id FROM resources WHERE parent = ? AND parent != id",
            parent
        )
        .fetch_all(executor)
        .await?
        .iter()
        .map(|r| r.id.clone().into())
        .collect();

        Ok(children)
    }

    pub async fn serialize_children_of<'c, E: sqlx::Executor<'c, Database = Sqlite>>(
        &self,
        parent: &ResourceId,
        executor: E,
    ) -> Result<Vec<u8>, ResourceStoreError> {
        let children = self.children_of(parent, executor).await?;
        let bincode = bincode::options().with_big_endian().with_varint_encoding();
        let res = bincode.serialize(&children)?;

        Ok(res)
    }

    pub async fn update_container_content<'c, E: sqlx::Executor<'c, Database = Sqlite>>(
        &self,
        parent: &ResourceId,
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
        id: &ResourceId,
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

    pub async fn create_root(&mut self) -> Result<(), ResourceStoreError> {
        let root = ResourceMetadata::new(
            &ROOT_ID,
            &ROOT_ID,
            ResourceKind::Container,
            "/",
            vec![],
            vec![Variant::new("default", "inode/directory", 0)],
        );
        self.create(&root, None).await
    }

    pub async fn get_root(
        &mut self,
    ) -> Result<(ResourceMetadata, Vec<ResourceMetadata>), ResourceStoreError> {
        self.get_container(&ROOT_ID).await
    }

    // Returns the whole set of object metadata from the root to the given object.
    // Will fail if a cycle is detected or if any parent id fails to return metadata.
    pub async fn get_full_path(
        &mut self,
        id: &ResourceId,
    ) -> Result<Vec<ResourceMetadata>, ResourceStoreError> {
        let mut res = vec![];
        let mut current = id.clone();
        let mut visited = HashSet::new();

        loop {
            if visited.contains(&current) {
                return Err(ResourceStoreError::ResourceCycle);
            }
            let meta = self.get_metadata(&current).await?;
            visited.insert(current.clone());
            let next = meta.parent();
            res.push(meta);
            if current.is_root() {
                break;
            }
            current = next;
        }

        // Make sure we order elements from root -> target node.
        res.reverse();
        Ok(res)
    }

    // Retrieve the list of objects matching the given name, optionnaly restricted to a given tag.
    // TODO: pagination
    pub async fn by_name(
        &self,
        name: &str,
        tag: Option<&str>,
    ) -> Result<Vec<ResourceId>, ResourceStoreError> {
        if name.trim().is_empty() {
            return Err(ResourceStoreError::Custom("EmptyNameQuery".into()));
        }

        let results: Vec<ResourceId> = if let Some(tag) = tag {
            sqlx::query_as(
                "SELECT resources.id FROM resources LEFT JOIN tags
                WHERE tags.tag = ? AND name = ? AND tags.id = resources.id ORDER BY frecency(resources.scorer) DESC",
            ).bind(name).bind(tag)
            .fetch_all(&self.db_pool)
            .await?
        } else {
            sqlx::query_as("SELECT id FROM resources WHERE name = ? ORDER BY frecency(scorer) DESC")
                .bind(name)
                .fetch_all(&self.db_pool)
                .await?
        };

        Ok(results)
    }

    // Retrieve the object with a given name and parent.
    pub async fn child_by_name(
        &mut self,
        parent: &ResourceId,
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
            Some(child) => self.get_metadata(&child.id.into()).await,
            None => Err(ResourceStoreError::NoSuchResource),
        }
    }

    // Retrieve the list of objects matching the given tag.
    // TODO: pagination
    pub async fn by_tag(&self, tag: &str) -> Result<Vec<ResourceId>, ResourceStoreError> {
        if tag.trim().is_empty() {
            return Err(ResourceStoreError::Custom("EmptyTagQuery".into()));
        }

        let results: Vec<ResourceId> = sqlx::query_as(
            r#"SELECT resources.id FROM resources
            LEFT JOIN tags
            WHERE tags.tag = ? and tags.id = resources.id
            ORDER BY frecency(resources.scorer) DESC"#,
        )
        .bind(tag)
        .fetch_all(&self.db_pool)
        .await?;

        Ok(results)
    }

    pub async fn by_text(
        &self,
        text: &str,
        tag: Option<String>,
    ) -> Result<Vec<IdFrec>, ResourceStoreError> {
        if text.trim().is_empty() {
            return Err(ResourceStoreError::Custom("EmptyTextQuery".into()));
        }

        self.fts.search(text, tag).await
    }

    pub async fn top_by_frecency(&self, count: u32) -> Result<Vec<IdFrec>, ResourceStoreError> {
        if count == 0 {
            return Err(ResourceStoreError::Custom("ZeroCountQuery".into()));
        }

        let results: Vec<IdFrec> = sqlx::query_as(
            "SELECT id, frecency(scorer) AS frecency FROM resources ORDER BY frecency DESC LIMIT ?",
        )
        .bind(count)
        .fetch_all(&self.db_pool)
        .await?;

        Ok(results)
    }

    pub async fn last_modified(&self, count: u32) -> Result<Vec<IdFrec>, ResourceStoreError> {
        if count == 0 {
            return Err(ResourceStoreError::Custom("ZeroCountQuery".into()));
        }

        let results: Vec<IdFrec> = sqlx::query_as(
            "SELECT id, frecency(scorer) AS frecency FROM resources ORDER BY modified DESC LIMIT ?",
        )
        .bind(count)
        .fetch_all(&self.db_pool)
        .await?;

        log::info!("last_modified({}): {:?}", count, results);
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
            tx = indexer.index(metadata, content, &self.fts, tx).await?
        }

        Ok(tx)
    }

    pub fn add_indexer(&mut self, indexer: Box<dyn Indexer + Send + Sync>) {
        self.indexers.push(indexer);
    }

    pub async fn close(&self) {
        self.db_pool.close().await
    }

    pub async fn create(
        &mut self,
        metadata: &ResourceMetadata,
        mut content: Option<VariantContent>,
    ) -> Result<(), ResourceStoreError> {
        self.check_container_leaf(&metadata.id(), &metadata.parent())
            .await?;

        // Start a transaction to store the new metadata.
        let tx = self.db_pool.begin().await?;
        let mut tx2 = self.create_metadata(metadata, tx).await?;

        // Update the children content of the parent if this is not creating the root.
        if !metadata.id().is_root() {
            self.update_container_content(&metadata.parent(), &mut tx2)
                .await?;
        }

        // If there is content run the text indexer for this mime type.
        let tx3 = if let Some(ref mut content) = content {
            self.update_text_index(metadata, &mut content.1, tx2)
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
        &mut self,
        metadata: &ResourceMetadata,
        mut content: Option<VariantContent>,
    ) -> Result<(), ResourceStoreError> {
        self.check_container_leaf(&metadata.id(), &metadata.parent())
            .await?;

        let mut tx = self.db_pool.begin().await?;
        let id = metadata.id();
        sqlx::query!("DELETE FROM resources WHERE id = ?", id)
            .execute(&mut tx)
            .await?;

        let mut tx2 = self.create_metadata(metadata, tx).await?;

        // Update the children content of the parent if this is not creating the root.
        if !metadata.id().is_root() {
            self.update_container_content(&metadata.parent(), &mut tx2)
                .await?;
        }

        // If there is content, run the text indexer for this mime type.
        let tx3 = if let Some(ref mut content) = content {
            self.update_text_index(metadata, &mut content.1, tx2)
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
        &mut self,
        id: &ResourceId,
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

    pub async fn delete(&mut self, id: &ResourceId) -> Result<(), ResourceStoreError> {
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
            self.update_container_content(&parent_id, &mut tx).await?;
            tx.commit().await?;
            self.evict_from_cache(id);
            return Ok(());
        }

        // Collect all the children, in a non-recursive way.

        // This set holds the list of all children to remove.
        let mut to_delete: HashSet<ResourceId> = HashSet::new();

        // This vector holds the list of remaining containers
        // that need to be checked.
        let mut containers: Vec<ResourceId> = vec![id.clone()];

        loop {
            let mut new_obj = vec![];

            for source_id in containers {
                let children: Vec<ResourceId> = self.children_of(&source_id, &self.db_pool).await?;

                for child in children {
                    // 1. add this child to the final set.
                    to_delete.insert(child.clone());
                    // 2. If it's a container, add it to the list of containers for the next iteration.
                    if self.is_container(&child).await? {
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
            self.store.delete(&child).await?;
            self.evict_from_cache(&child);
        }

        self.store.delete(id).await?;
        self.update_container_content(&parent_id, &mut tx).await?;
        tx.commit().await?;

        self.evict_from_cache(id);
        Ok(())
    }

    pub async fn get_metadata(
        &mut self,
        id: &ResourceId,
    ) -> Result<ResourceMetadata, ResourceStoreError> {
        // Check if we have this metadata in the LRU cache.
        if let Some(meta) = self.cache.get(id) {
            return Ok(meta.clone());
        }

        // Metadata can be retrieved fully from the SQL database.
        match sqlx::query!(
            r#"
    SELECT id, parent, kind, name, created, modified, scorer FROM resources
    WHERE id = ?"#,
            id
        )
        .fetch_one(&self.db_pool)
        .await
        {
            Ok(record) => {
                let mut meta = ResourceMetadata::new(
                    &record.id.into(),
                    &record.parent.into(),
                    record.kind.into(),
                    &record.name,
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

                self.update_cache(&meta);
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

                self.update_cache(&metadata);
                Ok(metadata)
            }
        }
    }

    pub async fn get_leaf(
        &mut self,
        id: &ResourceId,
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
        &mut self,
        id: &ResourceId,
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
                res.push(self.get_metadata(&child).await?);
            }

            Ok((meta, res))
        } else {
            // No children for this container.
            Ok((meta, vec![]))
        }
    }
}
