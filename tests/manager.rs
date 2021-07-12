use async_std::fs;
use chrono::Utc;
use costaeres::common::*;
use costaeres::config::Config;
use costaeres::file_store::FileStore;
use costaeres::manager::*;
use costaeres::scorer::{VisitEntry, VisitPriority};

static CONTENT: [u8; 100] = [0; 100];

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
        0.into(),
        ObjectKind::Container,
        10,
        "container",
        "text/plain",
        None,
    );
    manager
        .create(&container, Some(Box::new(&CONTENT[..])))
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
            .create(&child, Some(Box::new(&CONTENT[..])))
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
            .create(&child, Some(Box::new(&CONTENT[..])))
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
        0.into(),
        0.into(),
        ObjectKind::Leaf,
        10,
        "object 0",
        "text/plain",
        Some(vec!["one".into(), "two".into()]),
    );

    manager
        .create(&meta, Some(Box::new(&CONTENT[..])))
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
        0.into(),
        0.into(),
        ObjectKind::Leaf,
        100,
        "object 0 updated",
        "text/plain",
        Some(vec!["one".into(), "two".into(), "three".into()]),
    );
    let res = manager.update(&meta, Some(Box::new(&CONTENT[..]))).await;
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

#[async_std::test]
async fn rehydrate_single() {
    let (config, store) = prepare_test(2).await;

    // Adding an object to the file store
    let meta = ObjectMetadata::new(
        1.into(),
        0.into(),
        ObjectKind::Leaf,
        10,
        "object 0",
        "text/plain",
        Some(vec!["one".into(), "two".into()]),
    );
    store
        .create(&meta, Some(Box::new(&CONTENT[..])))
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
    let res = manager.create(&meta, Some(Box::new(&CONTENT[..]))).await;
    assert_eq!(res, Err(ObjectStoreError::InvalidContainerId));

    // Fail to store an object if the parent doesn't exist.
    let leaf_meta = ObjectMetadata::new(
        1.into(),
        0.into(),
        ObjectKind::Leaf,
        10,
        "leaf 1",
        "text/plain",
        None,
    );
    let res = manager
        .create(&leaf_meta, Some(Box::new(&CONTENT[..])))
        .await;
    assert_eq!(res, Err(ObjectStoreError::InvalidContainerId));

    // Create the root
    let root_meta = ObjectMetadata::new(
        0.into(),
        0.into(),
        ObjectKind::Container,
        10,
        "root",
        "text/plain",
        None,
    );
    manager
        .create(&root_meta, Some(Box::new(&CONTENT[..])))
        .await
        .unwrap();

    // And now add the leaf.
    manager
        .create(&leaf_meta, Some(Box::new(&CONTENT[..])))
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
        .create(&leaf_meta, Some(Box::new(&CONTENT[..])))
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
    assert_eq!(manager.has_object(0.into()).await.unwrap(), true);
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
    assert_eq!(root_meta.id(), 0.into());
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

    let root_path = manager.get_full_path(0.into()).await.unwrap();
    assert_eq!(root_path.len(), 1);
    assert_eq!(root_path[0].id(), 0.into());

    let obj_path = manager.get_full_path(30.into()).await.unwrap();
    assert_eq!(obj_path.len(), 4);
    assert_eq!(obj_path[0].id(), 0.into());
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

    let mut root_meta = manager.get_metadata(0.into()).await.unwrap();
    assert_eq!(root_meta.scorer().frecency(), 0);

    // Update the score
    root_meta.update_scorer(&VisitEntry::new(&Utc::now(), VisitPriority::Normal));
    manager.update(&root_meta, None).await.unwrap();
    let initial_score = root_meta.scorer().frecency();
    assert_eq!(initial_score, 100);

    // Clear the database to force re-hydration.
    manager.clear().await.unwrap();

    // Load the root again.
    let root_meta = manager.get_metadata(0.into()).await.unwrap();
    assert_eq!(initial_score, root_meta.scorer().frecency());
}
