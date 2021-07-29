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
    BoxedReader, ObjectId, ObjectKind, ObjectManager, ObjectMetadata, ObjectStore,
    ObjectStoreError, TransactionResult, ROOT_OBJECT_ID,
};
use crate::config::Config;
use crate::fts::Fts;
use crate::indexer::Indexer;
use async_std::fs::File;
use async_trait::async_trait;
use bincode::Options;
use chrono::{DateTime, Utc};
use log::{debug, error};
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::collections::{HashMap, HashSet};

pub struct Manager {
    db_pool: SqlitePool,
    store: Box<dyn ObjectStore + Send + Sync>,
    fts: Fts,
    indexers: HashMap<String, Box<dyn Indexer + Send + Sync>>, // Maps a content-type to an indexer.
}

impl Manager {
    pub async fn new(
        config: Config,
        store: Box<dyn ObjectStore + Send + Sync>,
    ) -> Result<Self, ObjectStoreError> {
        let _file = File::create(&config.db_path).await?;
        let db_pool = SqlitePool::connect(&format!("sqlite://{}", config.db_path)).await?;
        sqlx::migrate!("db/migrations")
            .run(&db_pool)
            .await
            .map_err(|err| ObjectStoreError::Custom(format!("Failed to run migration: {}", err)))?;

        let fts = Fts::new(&db_pool, 5);
        Ok(Manager {
            db_pool,
            store,
            fts,
            indexers: HashMap::new(),
        })
    }

    /// Use a existing transation to run the sql commands needed to create a metadata record.
    async fn create_metadata<'c>(
        &self,
        metadata: &ObjectMetadata,
        mut tx: Transaction<'c, Sqlite>,
    ) -> TransactionResult<'c> {
        let id = metadata.id();
        let parent = metadata.parent();
        let kind = metadata.kind();
        let name = metadata.name();
        let mime_type = metadata.mime_type();
        let size = metadata.size() as i64;
        let created = metadata.created();
        let modified = metadata.modified();
        let scorer = metadata.db_scorer();
        let frecency = metadata.scorer().frecency();
        sqlx::query!(
            r#"
    INSERT INTO objects ( id, parent, kind, name, mimeType, size, created, modified, scorer, frecency )
    VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
            "#,
            id,
            parent,
            kind,
            name,
            mime_type,
            size,
            created,
            modified,
            scorer,
            frecency,
        )
        .execute(&mut tx)
        .await?;

        // Insert the tags.
        if let Some(tags) = metadata.tags() {
            for tag in tags {
                sqlx::query!("INSERT INTO tags ( id, tag ) VALUES ( ?1, ?2 )", id, tag)
                    .execute(&mut tx)
                    .await?;
            }
        }

        // Insert the full text search data.
        let tx2 = self.fts.add_text(id, &name, tx).await?;

        Ok(tx2)
    }

    /// Returns `true` if this object id is in the local index.
    pub async fn has_object(&self, id: ObjectId) -> Result<bool, ObjectStoreError> {
        let count = sqlx::query_scalar!("SELECT count(*) FROM objects WHERE id = ?", id)
            .fetch_one(&self.db_pool)
            .await?;

        Ok(count == 1)
    }

    /// Returns the number of objects in the local index.
    pub async fn object_count(&self) -> Result<i32, ObjectStoreError> {
        let count = sqlx::query_scalar!("SELECT count(*) FROM objects")
            .fetch_one(&self.db_pool)
            .await?;

        Ok(count)
    }

    /// Returns `true` if this object id is in the local index and is a container.
    pub async fn is_container(&self, id: ObjectId) -> Result<bool, ObjectStoreError> {
        let count = sqlx::query_scalar!(
            "SELECT count(*) FROM objects WHERE id = ? and kind = ?",
            id,
            ObjectKind::Container
        )
        .fetch_one(&self.db_pool)
        .await?;

        Ok(count == 1)
    }

    /// Check container <-> leaf constraints
    // container == leaf is only valid for the root (container == 0)
    pub async fn check_container_leaf(
        &self,
        id: ObjectId,
        parent: ObjectId,
    ) -> Result<(), ObjectStoreError> {
        if parent == id && parent != ROOT_OBJECT_ID {
            error!("Only the root can be its own container.");
            return Err(ObjectStoreError::InvalidContainerId);
        }
        // Check that the parent is a known container, except when we create the root.
        if id != ROOT_OBJECT_ID && !self.is_container(parent).await? {
            error!("Object #{} is not a container", parent);
            return Err(ObjectStoreError::InvalidContainerId);
        }

        Ok(())
    }

    pub async fn children_of<'c, E: sqlx::Executor<'c, Database = Sqlite>>(
        &self,
        parent: ObjectId,
        executor: E,
    ) -> Result<Vec<ObjectId>, ObjectStoreError> {
        let children: Vec<ObjectId> = sqlx::query!(
            "SELECT id FROM objects WHERE parent = ? and parent != id",
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
        parent: ObjectId,
        executor: E,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        let children = self.children_of(parent, executor).await?;
        let bincode = bincode::options().with_big_endian().with_varint_encoding();
        let res = bincode.serialize(&children)?;

        Ok(res)
    }

    pub async fn update_container_content<'c, E: sqlx::Executor<'c, Database = Sqlite>>(
        &self,
        parent: ObjectId,
        executor: E,
    ) -> Result<(), ObjectStoreError> {
        let children = self.serialize_children_of(parent, executor).await?;
        self.store
            .update_content_from_slice(parent, &children)
            .await?;

        Ok(())
    }

    pub async fn parent_of<'c, E: sqlx::Executor<'c, Database = Sqlite>>(
        &self,
        id: ObjectId,
        executor: E,
    ) -> Result<ObjectId, ObjectStoreError> {
        let maybe_parent = sqlx::query!("SELECT parent FROM objects WHERE id = ?", id)
            .fetch_optional(executor)
            .await?;

        if let Some(record) = maybe_parent {
            return Ok(record.parent.into());
        }
        Err(ObjectStoreError::NoSuchObject)
    }

    pub async fn clear(&self) -> Result<(), ObjectStoreError> {
        let mut tx = self.db_pool.begin().await?;
        sqlx::query!("DELETE FROM objects").execute(&mut tx).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn create_root(&self) -> Result<(), ObjectStoreError> {
        let root = ObjectMetadata::new(
            0.into(),
            0.into(),
            ObjectKind::Container,
            0,
            "/",
            "inode/directory",
            None,
        );
        self.create(&root, None).await
    }

    pub async fn get_root(
        &self,
    ) -> Result<(ObjectMetadata, Vec<ObjectMetadata>), ObjectStoreError> {
        self.get_container(ROOT_OBJECT_ID).await
    }

    // Returns the whole set of object metadata from the root to the given object.
    // Will fail if a cycle is detected or if any parent id fails to return metadata.
    pub async fn get_full_path(
        &self,
        id: ObjectId,
    ) -> Result<Vec<ObjectMetadata>, ObjectStoreError> {
        let mut res = vec![];
        let mut current = id;
        let mut visited = HashSet::new();

        loop {
            if visited.contains(&current) {
                return Err(ObjectStoreError::ObjectCycle);
            }
            let meta = self.get_metadata(current).await?;
            visited.insert(current);
            let next = meta.parent();
            res.push(meta);
            if current == ROOT_OBJECT_ID {
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
    ) -> Result<Vec<ObjectId>, ObjectStoreError> {
        if name.trim().is_empty() {
            return Err(ObjectStoreError::Custom("EmptyNameQuery".into()));
        }

        let results: Vec<ObjectId> = if let Some(mime) = mime {
            sqlx::query!(
                "SELECT id FROM objects WHERE name = ? and mimeType = ? ORDER BY frecency DESC",
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
                "SELECT id FROM objects WHERE name = ? ORDER BY frecency DESC",
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

    // Retrieve the list of objects matching the given tag, optionnaly restricted to a given mime type.
    // TODO: pagination
    pub async fn by_tag(
        &self,
        tag: &str,
        mime: Option<&str>,
    ) -> Result<Vec<ObjectId>, ObjectStoreError> {
        if tag.trim().is_empty() {
            return Err(ObjectStoreError::Custom("EmptyTagQuery".into()));
        }

        let results: Vec<ObjectId> = if let Some(mime) = mime {
            sqlx::query!(
                r#"SELECT objects.id FROM objects
                   LEFT JOIN tags
                   WHERE tags.tag = ? and tags.id = objects.id and objects.mimeType = ?
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
                r#"SELECT objects.id FROM objects
            LEFT JOIN tags
            WHERE tags.tag = ? and tags.id = objects.id
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

    pub async fn by_text(&self, text: &str) -> Result<Vec<(ObjectId, u32)>, ObjectStoreError> {
        if text.trim().is_empty() {
            return Err(ObjectStoreError::Custom("EmptyTextQuery".into()));
        }

        self.fts.search(text).await
    }

    pub async fn top_by_frecency(
        &self,
        count: u32,
    ) -> Result<Vec<(ObjectId, u32)>, ObjectStoreError> {
        if count == 0 {
            return Err(ObjectStoreError::Custom("ZeroCountQuery".into()));
        }

        let results: Vec<(ObjectId, u32)> = sqlx::query!(
            "SELECT id, frecency FROM objects ORDER BY frecency DESC LIMIT ?",
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
        metadata: &'c ObjectMetadata,
        content: &mut BoxedReader,
        mut tx: Transaction<'c, Sqlite>,
    ) -> TransactionResult<'c> {
        if metadata.kind() == ObjectKind::Container {
            return Ok(tx);
        }

        if let Some(indexer) = self.indexers.get(&metadata.mime_type()) {
            tx = indexer.index(metadata.id(), content, &self.fts, tx).await?
        } else {
            debug!("No indexer available for {}", metadata.mime_type());
        }

        Ok(tx)
    }

    pub fn add_indexer(&mut self, mime_type: &str, indexer: Box<dyn Indexer + Send + Sync>) {
        let _ = self.indexers.insert(mime_type.into(), indexer);
    }
}

#[async_trait(?Send)]
impl ObjectManager for Manager {
    async fn next_id(&self) -> Result<ObjectId, ObjectStoreError> {
        let max = sqlx::query_scalar!("SELECT id FROM objects ORDER BY id DESC LIMIT 1")
            .fetch_one(&self.db_pool)
            .await?;

        Ok((max + 1).into())
    }

    async fn create(
        &self,
        metadata: &ObjectMetadata,
        mut content: Option<BoxedReader>,
    ) -> Result<(), ObjectStoreError> {
        self.check_container_leaf(metadata.id(), metadata.parent())
            .await?;

        // Start a transaction to store the new metadata.
        let tx = self.db_pool.begin().await?;
        let mut tx2 = self.create_metadata(metadata, tx).await?;

        // Update the children content of the parent if this is not creating the root.
        if metadata.id() != ROOT_OBJECT_ID {
            self.update_container_content(metadata.parent(), &mut tx2)
                .await?;
        }

        // If there is content run the text indexer for this mime type.
        let tx3 = if let Some(ref mut content) = content {
            self.update_text_index(&metadata, content, tx2).await?
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

    async fn update(
        &self,
        metadata: &ObjectMetadata,
        mut content: Option<BoxedReader>,
    ) -> Result<(), ObjectStoreError> {
        self.check_container_leaf(metadata.id(), metadata.parent())
            .await?;

        let mut tx = self.db_pool.begin().await?;
        let id = metadata.id();
        sqlx::query!("DELETE FROM objects where id = ?", id)
            .execute(&mut tx)
            .await?;

        let mut tx2 = self.create_metadata(metadata, tx).await?;

        // Update the children content of the parent if this is not creating the root.
        if metadata.id() != ROOT_OBJECT_ID {
            self.update_container_content(metadata.parent(), &mut tx2)
                .await?;
        }

        // If there is content, run the text indexer for this mime type.
        let tx3 = if let Some(ref mut content) = content {
            self.update_text_index(&metadata, content, tx2).await?
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

    async fn delete(&self, id: ObjectId) -> Result<(), ObjectStoreError> {
        let mut tx = self.db_pool.begin().await?;
        let is_container = self.is_container(id).await?;

        let parent_id = self.parent_of(id, &mut tx).await?;

        // Delete the object itself.
        // The tags will be removed by the delete cascade sql rule.
        sqlx::query!("DELETE FROM objects where id = ?", id)
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
        let mut to_delete: HashSet<ObjectId> = HashSet::new();

        // This vector holds the list of remaining containers
        // that need to be checked.
        let mut containers: Vec<ObjectId> = vec![id];

        loop {
            let mut new_obj = vec![];

            for source_id in containers {
                let children: Vec<ObjectId> = self.children_of(source_id, &self.db_pool).await?;

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
            sqlx::query!("DELETE FROM objects where id = ?", child)
                .execute(&mut tx)
                .await?;
            self.store.delete(child).await?;
        }

        self.store.delete(id).await?;
        self.update_container_content(parent_id, &mut tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn get_metadata(&self, id: ObjectId) -> Result<ObjectMetadata, ObjectStoreError> {
        // Metadata can be retrieved fully from the SQL database.
        // TODO: if that fails, try to re-hydrate from the object store.

        if let Ok(record) = sqlx::query!(
            r#"
    SELECT id, parent, kind, name, mimeType, size, created, modified, scorer  FROM objects
    WHERE id = ?
            "#,
            id
        )
        .fetch_one(&self.db_pool)
        .await
        {
            // Get the tags if any.
            let tags: Vec<String> = sqlx::query!("SELECT tag FROM tags WHERE id = ?", id)
                .fetch_all(&self.db_pool)
                .await?
                .iter()
                .map(|r| r.tag.clone())
                .collect();

            let mut meta = ObjectMetadata::new(
                record.id.into(),
                record.parent.into(),
                record.kind.into(),
                record.size,
                &record.name,
                &record.mimeType,
                None,
            );

            if !tags.is_empty() {
                meta.set_tags(Some(tags));
            }

            meta.set_created(DateTime::<Utc>::from_utc(record.created, Utc));
            meta.set_modified(DateTime::<Utc>::from_utc(record.modified, Utc));
            meta.set_scorer_from_db(&record.scorer);

            Ok(meta)
        } else {
            // Rehydrate from the file storage.
            debug!(
                "Object #{} not in index, fetching it from object storage.",
                id
            );
            let metadata = self.store.get_metadata(id).await?;
            let tx = self.db_pool.begin().await?;
            let tx2 = self.create_metadata(&metadata, tx).await?;
            tx2.commit().await?;

            Ok(metadata)
        }
    }

    async fn get_leaf(
        &self,
        id: ObjectId,
    ) -> Result<(ObjectMetadata, BoxedReader), ObjectStoreError> {
        let meta = self.get_metadata(id).await?;

        if meta.kind() != ObjectKind::Leaf {
            return Err(ObjectStoreError::NoSuchObject);
        }

        // Just relay content from the underlying store since we don't keep the content in the index.
        Ok((meta, self.store.get_content(id).await?))
    }

    async fn get_container(
        &self,
        id: ObjectId,
    ) -> Result<(ObjectMetadata, Vec<ObjectMetadata>), ObjectStoreError> {
        use async_std::io::ReadExt;

        let meta = self.get_metadata(id).await?;

        if meta.kind() != ObjectKind::Container {
            return Err(ObjectStoreError::NoSuchObject);
        }

        // Read the list of children from the container content.
        let mut file = self.store.get_content(id).await?;
        let mut buffer = vec![];
        file.read_to_end(&mut buffer).await?;
        let bincode = bincode::options().with_big_endian().with_varint_encoding();
        let children: Vec<ObjectId> = bincode.deserialize(&buffer)?;

        // Get the metadata for each child.
        let mut res = vec![];
        for child in children {
            res.push(self.get_metadata(child).await?);
        }

        Ok((meta, res))
    }
}
