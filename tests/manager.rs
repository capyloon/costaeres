use async_std::fs;
use costaeres::common::*;
use costaeres::config::Config;
use costaeres::file_store::FileStore;
use costaeres::manager::*;

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
    store.create(&meta, Box::new(&CONTENT[..])).await.unwrap();

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
    let res = manager.create(&meta, Box::new(&CONTENT[..])).await;
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
    let res = manager.create(&leaf_meta, Box::new(&CONTENT[..])).await;
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
        .create(&root_meta, Box::new(&CONTENT[..]))
        .await
        .unwrap();

    // And now add the leaf.
    manager
        .create(&leaf_meta, Box::new(&CONTENT[..]))
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
    let res = manager.create(&leaf_meta, Box::new(&CONTENT[..])).await;
    assert_eq!(res, Err(ObjectStoreError::InvalidContainerId));
}

#[async_std::test]
async fn delete_hierarchy() {
    let (config, store) = prepare_test(4).await;

    let manager = Manager::new(config, Box::new(store)).await.unwrap();

    // Adding an object to the file store
    let root = ObjectMetadata::new(
        0.into(),
        0.into(),
        ObjectKind::Container,
        10,
        "root",
        "text/plain",
        None,
    );
    manager.create(&root, Box::new(&CONTENT[..])).await.unwrap();

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
        .create(&container, Box::new(&CONTENT[..]))
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
            .create(&child, Box::new(&CONTENT[..]))
            .await
            .unwrap();
    }

    // Add a few children to the sub-container #10.
    for i in 25..35 {
        let child = ObjectMetadata::new(
            i.into(),
            10.into(),
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
            .create(&child, Box::new(&CONTENT[..]))
            .await
            .unwrap();
    }

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
        .create(&container, Box::new(&CONTENT[..]))
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
            .create(&child, Box::new(&CONTENT[..]))
            .await
            .unwrap();
    }

    // Add a few children to the sub-container #10.
    for i in 25..35 {
        let child = ObjectMetadata::new(
            i.into(),
            10.into(),
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
            .create(&child, Box::new(&CONTENT[..]))
            .await
            .unwrap();
    }

    assert_eq!(manager.object_count().await.unwrap(), 22);

    // Clear the local index.
    manager.clear().await.unwrap();

    assert_eq!(manager.object_count().await.unwrap(), 0);
}