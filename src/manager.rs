/// Main structure, managing the overall flow of operations.
/// It uses a SQL database to store the metadata, and relies
/// on another store to provide the content.
/// When a request fails at the database level (eg. unknown object id),
/// it tries to re-hydrade the database by fetching the metadata
/// from the store.
/// The manager knows about Container and Leafs, and will do the appropriate
/// operations to maintain this data properly. That allows to get all the tree
/// structure without hitting the remote store.
/// Any failure of the remote side leads to a rollback of the database transaction
/// to preserve the matching between both sides.
use crate::common::{
    ObjectId, ObjectKind, ObjectMetadata, ObjectStore, ObjectStoreError, ROOT_OBJECT_ID,
};
use crate::config::Config;
use async_std::{fs::File, io::Read};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{debug, error};
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::collections::HashSet;

pub struct Manager {
    db_pool: SqlitePool,
    store: Box<dyn ObjectStore>,
}

impl Manager {
    pub async fn new(config: Config, store: Box<dyn ObjectStore>) -> Result<Self, ()> {
        let _file = File::create(&config.db_path)
            .await
            .map_err(|err| error!("Failed to create db file: {}", err))?;
        let db_pool = SqlitePool::connect(&format!("sqlite://{}", config.db_path))
            .await
            .map_err(|err| error!("Failed to create pool for {}: {}", config.db_path, err))?;
        sqlx::migrate!("db/migrations")
            .run(&db_pool)
            .await
            .map_err(|err| error!("Failed to run migration: {}", err))?;

        Ok(Manager { db_pool, store })
    }

    /// Use a existing transation to run the sql commands needed to create a metadata record.
    async fn create_metadata<'c>(
        &self,
        metadata: &ObjectMetadata,
        mut tx: Transaction<'c, Sqlite>,
    ) -> Result<Transaction<'c, Sqlite>, ObjectStoreError> {
        let id = metadata.id();
        let parent = metadata.parent();
        let kind = metadata.kind();
        let name = metadata.name();
        let mime_type = metadata.mime_type();
        let size = metadata.size() as i64;
        let created = metadata.created();
        let modified = metadata.modified();
        let id = sqlx::query!(
            r#"
    INSERT INTO objects ( id, parent, kind, name, mimeType, size, created, modified )
    VALUES ( ?1, ?2, ?3, ?4, ?5,?6, ?7, ?8 )
            "#,
            id,
            parent,
            kind,
            name,
            mime_type,
            size,
            created,
            modified,
        )
        .execute(&mut tx)
        .await?
        .last_insert_rowid();

        // Insert the tags.
        if let Some(tags) = metadata.tags() {
            for tag in tags {
                sqlx::query!("INSERT INTO tags ( id, tag ) VALUES ( ?1, ?2 )", id, tag)
                    .execute(&mut tx)
                    .await?;
            }
        }

        Ok(tx)
    }

    /// Returns `true` if this object id is in the local index.
    pub async fn has_object(&self, id: ObjectId) -> Result<bool, ObjectStoreError> {
        let count = sqlx::query_scalar!("SELECT count(*) FROM objects WHERE id = ?", id)
            .fetch_one(&self.db_pool)
            .await?;

        Ok(count == 1)
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
}

#[async_trait(?Send)]
impl ObjectStore for Manager {
    async fn create(
        &self,
        metadata: &ObjectMetadata,
        content: Box<dyn Read + Unpin>,
    ) -> Result<(), ObjectStoreError> {
        self.check_container_leaf(metadata.id(), metadata.parent())
            .await?;

        // Start a transaction to store the new metadata.
        let tx = self.db_pool.begin().await?;
        let tx2 = self.create_metadata(metadata, tx).await?;

        // Create the store entry, and commit the SQlite transaction in case of success.
        match self.store.create(metadata, content).await {
            Ok(_) => {
                tx2.commit().await?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn update(
        &self,
        metadata: &ObjectMetadata,
        content: Box<dyn Read + Unpin>,
    ) -> Result<(), ObjectStoreError> {
        self.check_container_leaf(metadata.id(), metadata.parent())
            .await?;

        let mut tx = self.db_pool.begin().await?;
        let id = metadata.id();
        sqlx::query!("DELETE FROM objects where id = ?", id)
            .execute(&mut tx)
            .await?;

        let tx2 = self.create_metadata(metadata, tx).await?;

        match self.store.update(metadata, content).await {
            Ok(_) => {
                tx2.commit().await?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn delete(&self, id: ObjectId) -> Result<(), ObjectStoreError> {
        let mut tx = self.db_pool.begin().await?;
        let is_container = self.is_container(id).await?;

        // Delete the object itself.
        // The tags will be removed by the delete cascade sql rule.
        sqlx::query!("DELETE FROM objects where id = ?", id)
            .execute(&mut tx)
            .await?;

        if !is_container {
            self.store.delete(id).await?;
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
                let children: Vec<ObjectId> =
                    sqlx::query!("SELECT id FROM objects WHERE parent = ?", source_id)
                        .fetch_all(&self.db_pool)
                        .await?
                        .iter()
                        .map(|r| r.id.into())
                        .collect();

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
        tx.commit().await?;
        Ok(())
    }

    async fn get_metadata(&self, id: ObjectId) -> Result<ObjectMetadata, ObjectStoreError> {
        // Metadata can be retrieved fully from the SQL database.
        // TODO: if that fails, try to re-hydrate from the object store.

        if let Ok(record) = sqlx::query!(
            r#"
    SELECT id, parent, kind, name, mimeType, size, created, modified  FROM objects
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

    async fn get_full(
        &self,
        id: ObjectId,
    ) -> Result<(ObjectMetadata, Box<dyn Read>), ObjectStoreError> {
        self.store.get_full(id).await
    }
}
