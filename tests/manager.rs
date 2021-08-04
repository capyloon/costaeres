use async_std::fs;
use chrono::Utc;
use costaeres::common::*;
use costaeres::config::Config;
use costaeres::file_store::FileStore;
use costaeres::indexer::*;
use costaeres::manager::*;
use costaeres::scorer::{VisitEntry, VisitPriority};

async fn create_content() -> BoxedReader {
    let file = fs::File::open("./create_db.sh").await.unwrap();
    Box::new(file)
}

// Prepare a test directory, and returns the matching config and file store.
async fn prepare_test(index: u32) -> (Config, FileStore) {
    let _ = env_logger::try_init();

    let path = format!("./test-content/{}", index);

    let _ = fs::remove_dir_all(&path).await;
    let _ = fs::create_dir_all(&path).await;

    let store = FileStore::new(&path).await.unwrap();

    let config = Config {
        db_path: format!("{}/test_db.sqlite", &path),
        data_dir: ".".into(),
    };

    (config, store)
}

async fn create_hierarchy(manager: &Manager) {
    // Adding the root to the file store.
    manager.create_root().await.unwrap();

    // Add a sub-container.
    let container = ObjectMetadata::new(
        1.into(),
        ROOT_OBJECT_ID,
        ObjectKind::Container,
        10,
        "container",
        "text/plain",
        None,
    );
    manager
        .create(&container, Some(create_content().await))
        .await
        .unwrap();

    // Add a few children to the container.
    for i in 5..15 {
        let child = ObjectMetadata::new(
            i.into(),
            1.into(),
            if i == 10 {
                ObjectKind::Container
            } else {
                ObjectKind::Leaf
            },
            10,
            &format!("child #{}", i),
            "text/plain",
            None,
        );
        manager
            .create(&child, Some(create_content().await))
            .await
            .unwrap();
    }

    // Add a few children to the sub-container #10.
    for i in 25..35 {
        let child = ObjectMetadata::new(
            i.into(),
            10.into(),
            ObjectKind::Leaf,
            10,
            &format!("child #{}", i),
            "text/plain",
            Some(vec!["sub-child".into()]),
        );
        manager
            .create(&child, Some(create_content().await))
            .await
            .unwrap();
    }
}

#[async_std::test]
async fn basic_manager() {
    let (config, store) = prepare_test(1).await;

    let manager = Manager::new(config, Box::new(store)).await;
    assert!(manager.is_ok(), "Failed to create a manager");
    let manager = manager.unwrap();

    // Adding an object.
    let meta = ObjectMetadata::new(
        ROOT_OBJECT_ID,
        ROOT_OBJECT_ID,
        ObjectKind::Leaf,
        10,
        "object 0",
        "text/plain",
        Some(vec!["one".into(), "two".into()]),
    );

    manager
        .create(&meta, Some(create_content().await))
        .await
        .unwrap();
    // assert_eq!(res, Ok(()));

    let res = manager.get_metadata(meta.id()).await.unwrap();
    assert_eq!(res, meta);

    // Delete a non-existent object.
    let res = manager.delete(42.into()).await;
    assert!(res.is_err());

    // Update the root object.
    let meta = ObjectMetadata::new(
        ROOT_OBJECT_ID,
        ROOT_OBJECT_ID,
        ObjectKind::Leaf,
        100,
        "object 0 updated",
        "text/plain",
        Some(vec!["one".into(), "two".into(), "three".into()]),
    );
    let res = manager.update(&meta, Some(create_content().await)).await;
    assert_eq!(res, Ok(()));

    // Verify the updated metadata.
    let res = manager.get_metadata(meta.id()).await.unwrap();
    assert_eq!(res, meta);

    // Delete the root object
    let res = manager.delete(ROOT_OBJECT_ID).await;
    assert!(res.is_ok());

    // Expected failure
    let res = manager.get_metadata(meta.id()).await;
    assert!(res.is_err());
}

#[async_std::test]
async fn rehydrate_single() {
    let (config, store) = prepare_test(2).await;

    // Adding an object to the file store
    let meta = ObjectMetadata::new(
        1.into(),
        ROOT_OBJECT_ID,
        ObjectKind::Leaf,
        10,
        "object 0",
        "text/plain",
        Some(vec!["one".into(), "two".into()]),
    );
    store
        .create(&meta, Some(create_content().await))
        .await
        .unwrap();

    let manager = Manager::new(config, Box::new(store)).await.unwrap();

    assert_eq!(manager.has_object(meta.id()).await.unwrap(), false);

    let res = manager.get_metadata(meta.id()).await.unwrap();
    assert_eq!(res, meta);

    assert_eq!(manager.has_object(meta.id()).await.unwrap(), true);
}

#[async_std::test]
async fn check_constraints() {
    let (config, store) = prepare_test(3).await;

    let meta = ObjectMetadata::new(
        1.into(),
        1.into(),
        ObjectKind::Leaf,
        10,
        "object 0",
        "text/plain",
        None,
    );

    let manager = Manager::new(config, Box::new(store)).await.unwrap();

    // Fail to store an object where both id and parent are 1
    let res = manager.create(&meta, Some(create_content().await)).await;
    assert_eq!(res, Err(ObjectStoreError::InvalidContainerId));

    // Fail to store an object if the parent doesn't exist.
    let leaf_meta = ObjectMetadata::new(
        1.into(),
        ROOT_OBJECT_ID,
        ObjectKind::Leaf,
        10,
        "leaf 1",
        "text/plain",
        None,
    );
    let res = manager
        .create(&leaf_meta, Some(create_content().await))
        .await;
    assert_eq!(res, Err(ObjectStoreError::InvalidContainerId));

    // Create the root
    let root_meta = ObjectMetadata::new(
        ROOT_OBJECT_ID,
        ROOT_OBJECT_ID,
        ObjectKind::Container,
        10,
        "root",
        "text/plain",
        None,
    );
    manager
        .create(&root_meta, Some(create_content().await))
        .await
        .unwrap();

    // And now add the leaf.
    manager
        .create(&leaf_meta, Some(create_content().await))
        .await
        .unwrap();

    // Try to update the leaf to a non-existent parent.
    let leaf_meta = ObjectMetadata::new(
        1.into(),
        2.into(),
        ObjectKind::Leaf,
        10,
        "leaf 1",
        "text/plain",
        None,
    );
    let res = manager
        .create(&leaf_meta, Some(create_content().await))
        .await;
    assert_eq!(res, Err(ObjectStoreError::InvalidContainerId));
}

#[async_std::test]
async fn delete_hierarchy() {
    let (config, store) = prepare_test(4).await;

    let manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&manager).await;

    // Delete a single child.
    manager.delete(12.into()).await.unwrap();
    // Child 12 disappears
    assert_eq!(manager.has_object(12.into()).await.unwrap(), false);

    // Child 10 exists now.
    assert_eq!(manager.has_object(10.into()).await.unwrap(), true);

    // Delete the container.
    manager.delete(1.into()).await.unwrap();
    // Child 10 disappears, but not the root.
    assert_eq!(manager.has_object(10.into()).await.unwrap(), false);
    assert_eq!(manager.has_object(ROOT_OBJECT_ID).await.unwrap(), true);
}

#[async_std::test]
async fn rehydrate_full() {
    let (config, store) = prepare_test(5).await;

    let manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&manager).await;

    assert_eq!(manager.object_count().await.unwrap(), 22);

    // Clear the local index.
    manager.clear().await.unwrap();
    assert_eq!(manager.object_count().await.unwrap(), 0);

    let (root_meta, children) = manager.get_root().await.unwrap();
    assert_eq!(root_meta.id(), ROOT_OBJECT_ID);
    assert_eq!(children.len(), 1);

    let (sub_meta, children) = manager.get_container(children[0].id()).await.unwrap();
    assert_eq!(sub_meta.id(), 1.into());
    assert_eq!(children.len(), 10);
}

#[async_std::test]
async fn get_full_path() {
    let (config, store) = prepare_test(6).await;

    let manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&manager).await;

    let root_path = manager.get_full_path(ROOT_OBJECT_ID).await.unwrap();
    assert_eq!(root_path.len(), 1);
    assert_eq!(root_path[0].id(), ROOT_OBJECT_ID);

    let obj_path = manager.get_full_path(30.into()).await.unwrap();
    assert_eq!(obj_path.len(), 4);
    assert_eq!(obj_path[0].id(), ROOT_OBJECT_ID);
    assert_eq!(obj_path[1].id(), 1.into());
    assert_eq!(obj_path[2].id(), 10.into());
    assert_eq!(obj_path[3].id(), 30.into());
}

#[async_std::test]
async fn search_by_name() {
    let (config, store) = prepare_test(7).await;

    let manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&manager).await;

    let results = manager.by_name("unknown", Some("image/png")).await.unwrap();
    assert_eq!(results.len(), 0);

    let results = manager.by_name("child #12", None).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], 12.into());

    let results = manager
        .by_name("child #12", Some("text/plain"))
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], 12.into());

    let results = manager
        .by_name("child #12", Some("image/png"))
        .await
        .unwrap();
    assert_eq!(results.len(), 0);
}

#[async_std::test]
async fn search_by_tag() {
    let (config, store) = prepare_test(8).await;

    let manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&manager).await;

    let results = manager.by_tag("no-such-tag", None).await.unwrap();
    assert_eq!(results.len(), 0);

    let results = manager.by_tag("sub-child", None).await.unwrap();
    assert_eq!(results.len(), 10);
    assert_eq!(results[0], 25.into());

    let results = manager
        .by_tag("sub-child", Some("text/plain"))
        .await
        .unwrap();
    assert_eq!(results.len(), 10);
    assert_eq!(results[0], 25.into());

    let results = manager
        .by_tag("sub-child", Some("image/png"))
        .await
        .unwrap();
    assert_eq!(results.len(), 0);
}

#[async_std::test]
async fn search_by_text() {
    let (config, store) = prepare_test(9).await;

    let manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&manager).await;

    let results = manager.by_text("no-match").await.unwrap();
    assert_eq!(results.len(), 0);

    let results = manager.by_text("cont").await.unwrap();
    assert_eq!(results.len(), 1);

    let results = manager.by_text("child").await.unwrap();
    assert_eq!(results.len(), 20);

    let results = manager.by_text("child #27").await.unwrap();
    assert_eq!(results.len(), 1);

    let results = manager.by_text("child #27 #27").await.unwrap();
    assert_eq!(results.len(), 1);

    let results = manager.by_text("child #17").await.unwrap();
    assert_eq!(results.len(), 0);
}

#[async_std::test]
async fn score() {
    let (config, store) = prepare_test(10).await;

    let manager = Manager::new(config, Box::new(store)).await.unwrap();
    manager.create_root().await.unwrap();

    let mut root_meta = manager.get_metadata(ROOT_OBJECT_ID).await.unwrap();
    assert_eq!(root_meta.scorer().frecency(), 0);

    // Update the score
    root_meta.update_scorer(&VisitEntry::new(&Utc::now(), VisitPriority::Normal));
    manager.update(&root_meta, None).await.unwrap();
    let initial_score = root_meta.scorer().frecency();
    assert_eq!(initial_score, 100);

    // Clear the database to force re-hydration.
    manager.clear().await.unwrap();

    // Load the root again.
    let root_meta = manager.get_metadata(ROOT_OBJECT_ID).await.unwrap();
    assert_eq!(initial_score, root_meta.scorer().frecency());
}

#[async_std::test]
async fn top_frecency() {
    let (config, store) = prepare_test(11).await;

    let manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&manager).await;

    let mut root_meta = manager.get_metadata(ROOT_OBJECT_ID).await.unwrap();
    assert_eq!(root_meta.scorer().frecency(), 0);

    // Update the score
    root_meta.update_scorer(&VisitEntry::new(&Utc::now(), VisitPriority::Normal));
    manager.update(&root_meta, None).await.unwrap();
    assert_eq!(root_meta.scorer().frecency(), 100);

    let results = manager.top_by_frecency(10).await.unwrap();
    assert_eq!(results.len(), 10);
    let first = results[0];
    assert_eq!(first, (ROOT_OBJECT_ID, 100));
}

#[async_std::test]
async fn index_places() {
    let (config, store) = prepare_test(12).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();
    manager.add_indexer(
        "application/x-places+json",
        Box::new(create_places_indexer()),
    );

    manager.create_root().await.unwrap();
    let leaf_meta = ObjectMetadata::new(
        1.into(),
        ROOT_OBJECT_ID,
        ObjectKind::Leaf,
        10,
        "ecdf525a-e5d6-11eb-9c9b-d3fd1d0ea335",
        "application/x-places+json",
        None,
    );

    let places1 = fs::File::open("./test-fixtures/places-1.json")
        .await
        .unwrap();

    manager
        .create(&leaf_meta, Some(Box::new(places1)))
        .await
        .unwrap();

    // Found in the url.
    let results = manager.by_text("example").await.unwrap();
    assert_eq!(results.len(), 1);

    // Found in the title.
    let results = manager.by_text("web").await.unwrap();
    assert_eq!(results.len(), 1);

    // Update the object with new content.
    let places2 = fs::File::open("./test-fixtures/places-2.json")
        .await
        .unwrap();
    manager
        .update(&leaf_meta, Some(Box::new(places2)))
        .await
        .unwrap();

    // Found in the url.
    let results = manager.by_text("example").await.unwrap();
    assert_eq!(results.len(), 1);

    // Not found in the title anymore.
    let results = manager.by_text("web").await.unwrap();
    assert_eq!(results.len(), 0);

    // Found in the new title.
    let results = manager.by_text("new").await.unwrap();
    assert_eq!(results.len(), 1);

    // Delete the object, removing the associated text index.
    manager.delete(leaf_meta.id()).await.unwrap();

    // Used to be found in the url.
    let results = manager.by_text("example").await.unwrap();
    assert_eq!(results.len(), 0);

    // Used to be found in the title.
    let results = manager.by_text("new").await.unwrap();
    assert_eq!(results.len(), 0);
}

#[async_std::test]
async fn index_contacts() {
    let (config, store) = prepare_test(13).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();
    manager.add_indexer(
        "application/x-contacts+json",
        Box::new(create_contacts_indexer()),
    );

    manager.create_root().await.unwrap();
    let leaf_meta = ObjectMetadata::new(
        1.into(),
        ROOT_OBJECT_ID,
        ObjectKind::Leaf,
        10,
        "ecdf525a-e5d6-11eb-9c9b-d3fd1d0ea335",
        "application/x-contacts+json",
        None,
    );

    let places1 = fs::File::open("./test-fixtures/contacts-1.json")
        .await
        .unwrap();

    manager
        .create(&leaf_meta, Some(Box::new(places1)))
        .await
        .unwrap();

    // Found in the name.
    let results = manager.by_text("jean").await.unwrap();
    assert_eq!(results.len(), 1);

    // Found in the phone number.
    let results = manager.by_text("4567").await.unwrap();
    assert_eq!(results.len(), 1);

    // Found in the name and email.
    let results = manager.by_text("dupont").await.unwrap();
    assert_eq!(results.len(), 1);

    // Found in the email.
    let results = manager.by_text("secret").await.unwrap();
    assert_eq!(results.len(), 1);
}

#[async_std::test]
async fn id_generation() {
    let (config, store) = prepare_test(14).await;

    let manager = Manager::new(config, Box::new(store)).await.unwrap();

    manager.create_root().await.unwrap();
    let next_id = manager.next_id().await.unwrap();
    assert_eq!(next_id, 1.into());

    manager.delete(ROOT_OBJECT_ID).await.unwrap();
    assert_eq!(manager.object_count().await.unwrap(), 0);

    create_hierarchy(&manager).await;
    let next_id = manager.next_id().await.unwrap();
    assert_eq!(next_id, 35.into());
}

#[async_std::test]

async fn get_root_children() {
    let (config, store) = prepare_test(15).await;

    let manager = Manager::new(config, Box::new(store)).await;
    assert!(manager.is_ok(), "Failed to create a manager");
    let manager = manager.unwrap();

    manager.create_root().await.unwrap();

    let (root, children) = manager.get_container(ROOT_OBJECT_ID).await.unwrap();

    assert_eq!(root.id(), ROOT_OBJECT_ID);
    assert_eq!(children.len(), 0);
}

#[async_std::test]

async fn unique_children_names() {
    let (config, store) = prepare_test(16).await;

    let manager = Manager::new(config, Box::new(store)).await;
    assert!(manager.is_ok(), "Failed to create a manager");
    let manager = manager.unwrap();

    manager.create_root().await.unwrap();

    let leaf_meta = ObjectMetadata::new(
        1.into(),
        ROOT_OBJECT_ID,
        ObjectKind::Leaf,
        10,
        "file.txt",
        "text/plain",
        None,
    );

    manager.create(&leaf_meta, None).await.unwrap();

    let res = manager.create(&leaf_meta, None).await;
    assert!(res.is_err());
}

#[async_std::test]

async fn child_by_name() {
    let (config, store) = prepare_test(16).await;

    let manager = Manager::new(config, Box::new(store)).await;
    assert!(manager.is_ok(), "Failed to create a manager");
    let manager = manager.unwrap();

    manager.create_root().await.unwrap();

    let leaf_meta = ObjectMetadata::new(
        1.into(),
        ROOT_OBJECT_ID,
        ObjectKind::Leaf,
        10,
        "file.txt",
        "text/plain",
        None,
    );

    manager.create(&leaf_meta, None).await.unwrap();

    let leaf_meta = ObjectMetadata::new(
        2.into(),
        ROOT_OBJECT_ID,
        ObjectKind::Leaf,
        10,
        "photo.png",
        "image/png",
        None,
    );

    manager.create(&leaf_meta, None).await.unwrap();

    let file = manager
        .child_by_name(ROOT_OBJECT_ID, "file.txt")
        .await
        .unwrap();
    assert_eq!(file.mime_type(), "text/plain");

    let image = manager
        .child_by_name(ROOT_OBJECT_ID, "photo.png")
        .await
        .unwrap();
    assert_eq!(image.mime_type(), "image/png");
}

#[async_std::test]

async fn migration_check() {
    let (config, store) = prepare_test(17).await;

    {
        let manager = Manager::new(config.clone(), Box::new(store.clone())).await;
        assert!(manager.is_ok(), "Failed to create first manager");
        let manager = manager.unwrap();

        manager.create_root().await.unwrap();

        manager.close().await;
    }

    {
        let manager = Manager::new(config, Box::new(store)).await;
        assert!(manager.is_ok(), "Failed to create second manager");
        let manager = manager.unwrap();

        let has_root = manager.has_object(ROOT_OBJECT_ID).await.unwrap();
        assert_eq!(has_root, true);

        manager.close().await;
    }
}
