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
use crate::common::{ObjectId, ObjectMetadata, ObjectStore, ObjectStoreError};
use crate::config::Config;
use async_std::{fs::File, io::Read};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::error;
use sqlx::SqlitePool;

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

        Ok(Manager {
            db_pool,
            store,
        })
    }
}

#[async_trait(?Send)]
impl ObjectStore for Manager {
    async fn create(
        &self,
        metadata: &ObjectMetadata,
        content: Box<dyn Read + Unpin>,
    ) -> Result<(), ObjectStoreError> {
        // Start a transaction to store the new metadata.
        let mut tx = self.db_pool.begin().await?;

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

        // Create the store entry, and commit the SQlite transaction in case of success.
        match self.store.create(metadata, content).await {
            Ok(_) => {
                tx.commit().await?;
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
        let mut tx = self.db_pool.begin().await?;
        let id = metadata.id();
        sqlx::query!("DELETE FROM objects where id = ?", id)
            .execute(&mut tx)
            .await?;

        let parent = metadata.parent();
        let kind = metadata.kind();
        let name = metadata.name();
        let mime_type = metadata.mime_type();
        let size = metadata.size() as i64;
        let created = metadata.created();
        let modified = metadata.modified();
        let _ = sqlx::query!(
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

        match self.store.update(metadata, content).await {
            Ok(_) => {
                tx.commit().await?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn delete(&self, id: ObjectId) -> Result<(), ObjectStoreError> {
        let mut tx = self.db_pool.begin().await?;
        sqlx::query!("DELETE FROM objects where id = ?", id)
            .execute(&mut tx)
            .await?;

        match self.store.delete(id).await {
            Ok(_) => {
                tx.commit().await?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn get_metadata(&self, id: ObjectId) -> Result<ObjectMetadata, ObjectStoreError> {
        // Metadata can be retrieved fully from the SQL database.
        // TODO: if that fails, try to re-hydrate from the object store.

        let record = sqlx::query!(
            r#"
    SELECT id, parent, kind, name, mimeType, size, created, modified  FROM objects
    WHERE id = ?
            "#,
            id
        )
        .fetch_one(&self.db_pool)
        .await?;

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

        // self.store.get_metadata(id).await
    }

    async fn get_full(
        &self,
        id: ObjectId,
    ) -> Result<(ObjectMetadata, Box<dyn Read>), ObjectStoreError> {
        self.store.get_full(id).await
    }
}

#[async_std::test]
async fn basic_manager() {
    use crate::common::{ObjectKind, ObjectStore};
    use crate::file_store::FileStore;
    use async_std::fs;

    static CONTENT: [u8; 100] = [0; 100];

    let _ = env_logger::try_init();

    let _ = fs::remove_dir_all("./test-content/1").await;
    let _ = fs::create_dir_all("./test-content/1").await;

    let store = FileStore::new("./test-content/1").await.unwrap();

    let config = Config {
        db_path: "./test-content/1/test_create_db.sqlite".into(),
        data_dir: ".".into(),
    };

    let manager = Manager::new(config, Box::new(store)).await;
    assert!(manager.is_ok(), "Failed to create a manager");
    let manager = manager.unwrap();

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

    let res = manager.create(&meta, Box::new(&CONTENT[..])).await;
    assert_eq!(res, Ok(()));

    let res = manager.get_metadata(meta.id()).await.unwrap();
    assert_eq!(res, meta);

    // Delete a non-existent object.
    let res = manager.delete(42.into()).await;
    assert!(res.is_err());

    // Update the root object.
    let meta = ObjectMetadata::new(
        0.into(),
        0.into(),
        ObjectKind::Leaf,
        100,
        "object 0 updated",
        "text/plain",
        Some(vec!["one".into(), "two".into(), "three".into()]),
    );
    let res = manager.update(&meta, Box::new(&CONTENT[..])).await;
    assert_eq!(res, Ok(()));

    // Verify the updated metadata.
    let res = manager.get_metadata(meta.id()).await.unwrap();
    assert_eq!(res, meta);

    // Delete the root object
    let res = manager.delete(0.into()).await;
    assert!(res.is_ok());

    // Expected failure
    let res = manager.get_metadata(meta.id()).await;
    assert!(res.is_err());
}
