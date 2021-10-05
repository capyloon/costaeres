use async_std::fs;
use chrono::Utc;
use costaeres::common::*;
use costaeres::config::Config;
use costaeres::file_store::FileStore;
use costaeres::indexer::*;
use costaeres::manager::*;
use costaeres::scorer::{VisitEntry, VisitPriority};

fn named_variant(name: &str, mime_type: &str) -> Variant {
    Variant::new(name, mime_type, 42)
}

fn default_variant() -> Variant {
    named_variant("default", "application/octet-stream")
}

async fn named_content(name: &str) -> VariantContent {
    let file = fs::File::open("./create_db.sh").await.unwrap();
    VariantContent::new(
        named_variant(name, "application/octet-stream"),
        Box::new(file),
    )
}

async fn default_content() -> VariantContent {
    named_content("default").await
}

// Prepare a test directory, and returns the matching config and file store.
async fn prepare_test(index: u32) -> (Config, FileStore) {
    let _ = env_logger::try_init();

    let path = format!("./test-content/{}", index);

    let _ = fs::remove_dir_all(&path).await;
    let _ = fs::create_dir_all(&path).await;

    let store = FileStore::new(
        &path,
        Box::new(DefaultResourceNameProvider),
        Box::new(IdentityTransformer),
    )
    .await
    .unwrap();

    let config = Config {
        db_path: format!("{}/test_db.sqlite", &path),
        data_dir: ".".into(),
        metadata_cache_capacity: 100,
    };

    (config, store)
}

async fn create_hierarchy(manager: &mut Manager) {
    // Adding the root to the file store.
    manager.create_root().await.unwrap();

    // Add a sub-container.
    let mut container = ResourceMetadata::new(
        &1.into(),
        &ROOT_ID,
        ResourceKind::Container,
        "container",
        vec![],
        vec![],
    );
    manager
        .create(&mut container, Some(default_content().await))
        .await
        .unwrap();

    // Add a few children to the container.
    for i in 5..15 {
        let mut child = ResourceMetadata::new(
            &i.into(),
            &1.into(),
            if i == 10 {
                ResourceKind::Container
            } else {
                ResourceKind::Leaf
            },
            &format!("child #{}", i),
            vec![],
            vec![default_variant()],
        );
        manager
            .create(&mut child, Some(default_content().await))
            .await
            .unwrap();
    }

    // Add a few children to the sub-container #10.
    for i in 25..35 {
        let mut child = ResourceMetadata::new(
            &i.into(),
            &10.into(),
            ResourceKind::Leaf,
            &format!("child #{}", i),
            vec!["sub-child".into()],
            vec![default_variant()],
        );
        manager
            .create(&mut child, Some(default_content().await))
            .await
            .unwrap();
    }
}

#[async_std::test]
async fn basic_manager() {
    let (config, store) = prepare_test(1).await;

    let manager = Manager::new(config, Box::new(store)).await;
    assert!(manager.is_ok(), "Failed to create a manager");
    let mut manager = manager.unwrap();

    // Adding an object.
    let mut meta = ResourceMetadata::new(
        &ROOT_ID,
        &ROOT_ID,
        ResourceKind::Leaf,
        "object 0",
        vec!["one".into(), "two".into()],
        vec![default_variant()],
    );

    manager
        .create(&mut meta, Some(default_content().await))
        .await
        .unwrap();
    // assert_eq!(res, Ok(()));

    let res = manager.get_metadata(&meta.id()).await.unwrap();
    assert_eq!(res, meta);

    // Delete a non-existent object.
    let res = manager.delete(&42.into()).await;
    assert!(res.is_err());

    // Update the root object.
    let res = manager
        .update_variant(&ROOT_ID, default_content().await)
        .await;
    assert_eq!(res, Ok(()));

    // Delete the root object
    let res = manager.delete(&ROOT_ID).await;
    assert!(res.is_ok());

    // Expected failure
    let res = manager.get_metadata(&meta.id()).await;
    assert!(res.is_err());
}

#[async_std::test]
async fn rehydrate_single() {
    let (config, store) = prepare_test(2).await;

    // Adding an object to the file store
    let mut meta = ResourceMetadata::new(
        &1.into(),
        &ROOT_ID,
        ResourceKind::Leaf,
        "object 0",
        vec!["one".into(), "two".into()],
        vec![default_variant()],
    );
    store
        .create(&mut meta, Some(default_content().await))
        .await
        .unwrap();

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();

    assert_eq!(manager.has_object(&meta.id()).await.unwrap(), false);

    let res = manager.get_metadata(&meta.id()).await.unwrap();
    assert_eq!(res, meta);

    assert_eq!(manager.has_object(&meta.id()).await.unwrap(), true);
}

#[async_std::test]
async fn check_constraints() {
    let (config, store) = prepare_test(3).await;

    let mut meta = ResourceMetadata::new(
        &1.into(),
        &1.into(),
        ResourceKind::Leaf,
        "object 0",
        vec![],
        vec![],
    );

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();

    // Fail to store an object where both id and parent are 1
    let res = manager
        .create(&mut meta, Some(default_content().await))
        .await;
    assert_eq!(res, Err(ResourceStoreError::InvalidContainerId));

    // Fail to store an object if the parent doesn't exist.
    let mut leaf_meta = ResourceMetadata::new(
        &1.into(),
        &ROOT_ID,
        ResourceKind::Leaf,
        "leaf 1",
        vec![],
        vec![default_variant()],
    );
    let res = manager
        .create(&mut leaf_meta, Some(default_content().await))
        .await;
    assert_eq!(res, Err(ResourceStoreError::InvalidContainerId));

    // Create the root
    let mut root_meta = ResourceMetadata::new(
        &ROOT_ID,
        &ROOT_ID,
        ResourceKind::Container,
        "root",
        vec![],
        vec![default_variant()],
    );
    manager
        .create(&mut root_meta, Some(default_content().await))
        .await
        .unwrap();

    // And now add the leaf.
    manager
        .create(&mut leaf_meta, Some(default_content().await))
        .await
        .unwrap();

    // Try to update the leaf to a non-existent parent.
    let mut leaf_meta = ResourceMetadata::new(
        &1.into(),
        &2.into(),
        ResourceKind::Leaf,
        "leaf 1",
        vec![],
        vec![default_variant()],
    );
    let res = manager
        .create(&mut leaf_meta, Some(default_content().await))
        .await;
    assert_eq!(res, Err(ResourceStoreError::InvalidContainerId));
}

#[async_std::test]
async fn delete_hierarchy() {
    let (config, store) = prepare_test(4).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&mut manager).await;

    // Delete a single child.
    manager.delete(&12.into()).await.unwrap();
    // Child 12 disappears
    assert_eq!(manager.has_object(&12.into()).await.unwrap(), false);

    // Child 10 exists now.
    assert_eq!(manager.has_object(&10.into()).await.unwrap(), true);

    // Delete the container.
    manager.delete(&1.into()).await.unwrap();
    // Child 10 disappears, but not the root.
    assert_eq!(manager.has_object(&10.into()).await.unwrap(), false);
    assert_eq!(manager.has_object(&ROOT_ID).await.unwrap(), true);
}

#[async_std::test]
async fn rehydrate_full() {
    let (config, store) = prepare_test(5).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&mut manager).await;

    assert_eq!(manager.resource_count().await.unwrap(), 22);

    // Clear the local index.
    manager.clear().await.unwrap();
    assert_eq!(manager.resource_count().await.unwrap(), 0);

    let (root_meta, children) = manager.get_root().await.unwrap();
    assert!(root_meta.id().is_root());
    assert_eq!(children.len(), 1);

    let (sub_meta, children) = manager.get_container(&children[0].id()).await.unwrap();
    assert_eq!(sub_meta.id(), 1.into());
    assert_eq!(children.len(), 10);
}

#[async_std::test]
async fn get_full_path() {
    let (config, store) = prepare_test(6).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&mut manager).await;

    let root_path = manager.get_full_path(&ROOT_ID).await.unwrap();
    assert_eq!(root_path.len(), 1);
    assert!(root_path[0].id().is_root());

    let obj_path = manager.get_full_path(&30.into()).await.unwrap();
    assert_eq!(obj_path.len(), 4);
    assert!(obj_path[0].id().is_root());
    assert_eq!(obj_path[1].id(), 1.into());
    assert_eq!(obj_path[2].id(), 10.into());
    assert_eq!(obj_path[3].id(), 30.into());
}

#[async_std::test]
async fn search_by_name() {
    let (config, store) = prepare_test(7).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&mut manager).await;

    let results = manager.by_name("unknown", Some("image/png")).await.unwrap();
    assert_eq!(results.len(), 0);

    let results = manager.by_name("child #12", None).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], 12.into());

    let results = manager.by_name("child #12", None).await.unwrap();
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

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&mut manager).await;

    let results = manager.by_tag("no-such-tag").await.unwrap();
    assert_eq!(results.len(), 0);

    let results = manager.by_tag("sub-child").await.unwrap();
    assert_eq!(results.len(), 10);
    assert_eq!(results[0], 25.into());
}

#[async_std::test]
async fn search_by_text() {
    let (config, store) = prepare_test(9).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&mut manager).await;

    let results = manager.by_text("no-match", None).await.unwrap();
    assert_eq!(results.len(), 0);

    let results = manager.by_text("cont", None).await.unwrap();
    assert_eq!(results.len(), 1);

    let results = manager.by_text("child", None).await.unwrap();
    assert_eq!(results.len(), 20);

    let results = manager.by_text("child #27", None).await.unwrap();
    assert_eq!(results.len(), 1);

    let results = manager.by_text("child #27 #27", None).await.unwrap();
    assert_eq!(results.len(), 1);

    let results = manager.by_text("child #17", None).await.unwrap();
    assert_eq!(results.len(), 0);
}

#[async_std::test]
async fn score() {
    let (config, store) = prepare_test(10).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();
    manager.create_root().await.unwrap();

    let root_meta = manager.get_metadata(&ROOT_ID).await.unwrap();
    assert_eq!(root_meta.scorer().frecency(), 0);

    // Update the score
    manager
        .visit(
            &ROOT_ID,
            &VisitEntry::new(&Utc::now(), VisitPriority::Normal),
        )
        .await
        .unwrap();
    let root_meta = manager.get_metadata(&ROOT_ID).await.unwrap();
    let initial_score = root_meta.scorer().frecency();
    assert_eq!(initial_score, 100);

    // Clear the database to force re-hydration.
    manager.clear().await.unwrap();

    // Load the root again.
    let root_meta = manager.get_metadata(&ROOT_ID).await.unwrap();
    assert_eq!(initial_score, root_meta.scorer().frecency());
}

#[async_std::test]
async fn top_frecency() {
    let (config, store) = prepare_test(11).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&mut manager).await;

    let root_meta = manager.get_metadata(&ROOT_ID).await.unwrap();
    assert_eq!(root_meta.scorer().frecency(), 0);

    // Update the score
    manager
        .visit(
            &ROOT_ID,
            &VisitEntry::new(&Utc::now(), VisitPriority::Normal),
        )
        .await
        .unwrap();
    let root_meta = manager.get_metadata(&ROOT_ID).await.unwrap();
    assert_eq!(root_meta.scorer().frecency(), 100);

    let results = manager.top_by_frecency(10).await.unwrap();
    assert_eq!(results.len(), 10);
    assert_eq!(results[0], IdFrec::new(&ROOT_ID, 100));
}

#[async_std::test]
async fn index_places() {
    let (config, store) = prepare_test(12).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();
    manager.add_indexer(Box::new(create_places_indexer()));

    manager.create_root().await.unwrap();
    let mut leaf_meta = ResourceMetadata::new(
        &1.into(),
        &ROOT_ID,
        ResourceKind::Leaf,
        "ecdf525a-e5d6-11eb-9c9b-d3fd1d0ea335",
        vec!["places".into()],
        vec![],
    );

    let places1 = fs::File::open("./test-fixtures/places-1.json")
        .await
        .unwrap();

    manager
        .create(
            &mut leaf_meta,
            Some(VariantContent::new(
                named_variant("default", "application/x-places+json"),
                Box::new(places1),
            )),
        )
        .await
        .unwrap();

    // Found in the url.
    let results = manager
        .by_text("example", Some("places".into()))
        .await
        .unwrap();
    assert_eq!(results.len(), 1);

    // Found in the title.
    let results = manager.by_text("web", Some("places".into())).await.unwrap();
    assert_eq!(results.len(), 1);

    // Update the object with new content.
    let places2 = fs::File::open("./test-fixtures/places-2.json")
        .await
        .unwrap();
    manager
        .update_variant(
            &leaf_meta.id(),
            VariantContent::new(
                named_variant("default", "application/x-places+json"),
                Box::new(places2),
            ),
        )
        .await
        .unwrap();

    // Found in the url.
    let results = manager
        .by_text("example", Some("places".into()))
        .await
        .unwrap();
    assert_eq!(results.len(), 1);

    // Not found in the title anymore.
    let results = manager.by_text("web", Some("places".into())).await.unwrap();
    assert_eq!(results.len(), 0);

    // Found in the new title.
    let results = manager.by_text("new", Some("places".into())).await.unwrap();
    assert_eq!(results.len(), 1);

    // Delete the object, removing the associated text index.
    manager.delete(&leaf_meta.id()).await.unwrap();

    // Used to be found in the url.
    let results = manager
        .by_text("example", Some("places".into()))
        .await
        .unwrap();
    assert_eq!(results.len(), 0);

    // Used to be found in the title.
    let results = manager.by_text("new", Some("places".into())).await.unwrap();
    assert_eq!(results.len(), 0);
}

#[async_std::test]
async fn index_contacts() {
    let (config, store) = prepare_test(13).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();
    manager.add_indexer(Box::new(create_contacts_indexer()));

    manager.create_root().await.unwrap();
    let mut leaf_meta = ResourceMetadata::new(
        &1.into(),
        &ROOT_ID,
        ResourceKind::Leaf,
        "ecdf525a-e5d6-11eb-9c9b-d3fd1d0ea335",
        vec!["contact".into()],
        vec![default_variant()],
    );

    let contacts = fs::File::open("./test-fixtures/contacts-1.json")
        .await
        .unwrap();

    manager
        .create(
            &mut leaf_meta,
            Some(VariantContent::new(
                named_variant("default", "application/x-contact+json"),
                Box::new(contacts),
            )),
        )
        .await
        .unwrap();

    // Found in the name.
    let results = manager
        .by_text("jean", Some("contact".into()))
        .await
        .unwrap();
    assert_eq!(results.len(), 1);

    // Found in the phone number.
    let results = manager
        .by_text("4567", Some("contact".into()))
        .await
        .unwrap();
    assert_eq!(results.len(), 1);

    // Found in the name and email.
    let results = manager
        .by_text("dupont", Some("contact".into()))
        .await
        .unwrap();
    assert_eq!(results.len(), 1);

    // Found in the email.
    let results = manager
        .by_text("secret", Some("contact".into()))
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
}

#[async_std::test]

async fn get_root_children() {
    let (config, store) = prepare_test(15).await;

    let manager = Manager::new(config, Box::new(store)).await;
    assert!(manager.is_ok(), "Failed to create a manager");
    let mut manager = manager.unwrap();

    manager.create_root().await.unwrap();

    let (root, children) = manager.get_container(&ROOT_ID).await.unwrap();

    assert!(root.id().is_root());
    assert_eq!(children.len(), 0);
}

#[async_std::test]

async fn unique_children_names() {
    let (config, store) = prepare_test(16).await;

    let manager = Manager::new(config, Box::new(store)).await;
    assert!(manager.is_ok(), "Failed to create a manager");
    let mut manager = manager.unwrap();

    manager.create_root().await.unwrap();

    let mut leaf_meta = ResourceMetadata::new(
        &1.into(),
        &ROOT_ID,
        ResourceKind::Leaf,
        "file.txt",
        vec![],
        vec![],
    );

    manager.create(&mut leaf_meta, None).await.unwrap();

    let res = manager.create(&mut leaf_meta, None).await;
    assert!(res.is_err());
}

#[async_std::test]

async fn child_by_name() {
    let (config, store) = prepare_test(16).await;

    let manager = Manager::new(config, Box::new(store)).await;
    assert!(manager.is_ok(), "Failed to create a manager");
    let mut manager = manager.unwrap();

    manager.create_root().await.unwrap();

    let mut leaf_meta = ResourceMetadata::new(
        &1.into(),
        &ROOT_ID,
        ResourceKind::Leaf,
        "file.txt",
        vec![],
        vec![],
    );

    manager.create(&mut leaf_meta, None).await.unwrap();

    let mut leaf_meta = ResourceMetadata::new(
        &2.into(),
        &ROOT_ID,
        ResourceKind::Leaf,
        "photo.png",
        vec![],
        vec![],
    );

    manager.create(&mut leaf_meta, None).await.unwrap();

    let file = manager.child_by_name(&ROOT_ID, "file.txt").await.unwrap();
    assert_eq!(file.name(), "file.txt");

    let image = manager.child_by_name(&ROOT_ID, "photo.png").await.unwrap();
    assert_eq!(image.name(), "photo.png");
}

#[async_std::test]

async fn migration_check() {
    let (config, store) = prepare_test(17).await;
    let (_config, store2) = prepare_test(17).await;

    {
        let manager = Manager::new(config.clone(), Box::new(store)).await;
        assert!(manager.is_ok(), "Failed to create first manager");
        let mut manager = manager.unwrap();

        manager.create_root().await.unwrap();

        manager.close().await;
    }

    {
        let manager = Manager::new(config, Box::new(store2)).await;
        assert!(manager.is_ok(), "Failed to create second manager");
        let manager = manager.unwrap();

        let has_root = manager.has_object(&ROOT_ID).await.unwrap();
        assert_eq!(has_root, true);

        manager.close().await;
    }
}

#[async_std::test]
async fn frecency_update() {
    let (config, store) = prepare_test(18).await;

    let manager = Manager::new(config.clone(), Box::new(store)).await;
    assert!(manager.is_ok(), "Failed to create first manager");
    let mut manager = manager.unwrap();

    manager.create_root().await.unwrap();

    let meta = manager.get_metadata(&ROOT_ID).await.unwrap();
    assert_eq!(meta.scorer().frecency(), 0);

    manager
        .visit(&meta.id(), &VisitEntry::now(VisitPriority::Normal))
        .await
        .unwrap();
    let meta = manager.get_metadata(&ROOT_ID).await.unwrap();
    assert_eq!(meta.scorer().frecency(), 100);
}

#[async_std::test]
async fn index_places_mdn() {
    let (config, store) = prepare_test(19).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();
    manager.add_indexer(Box::new(create_places_indexer()));

    manager.create_root().await.unwrap();
    let mut leaf_meta = ResourceMetadata::new(
        &1.into(),
        &ROOT_ID,
        ResourceKind::Leaf,
        "ecdf525a-e5d6-11eb-9c9b-d3fd1d0ea335",
        vec!["places".into()],
        vec![],
    );

    let places1 = fs::File::open("./test-fixtures/places-mdn.json")
        .await
        .unwrap();

    manager
        .create(
            &mut leaf_meta,
            Some(VariantContent::new(
                named_variant("default", "application/x-places+json"),
                Box::new(places1),
            )),
        )
        .await
        .unwrap();

    // Found in the url.
    let results = manager.by_text("mdn", Some("places".into())).await.unwrap();
    assert_eq!(results.len(), 1);
}

#[async_std::test]
async fn import_from_path() {
    let (config, store) = prepare_test(20).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();

    manager.create_root().await.unwrap();

    // Wrong file.
    let meta = manager
        .import_from_path(&ROOT_ID, "./test-fixtures/unknown.txt", false)
        .await;
    assert_eq!(
        meta,
        Err(ResourceStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No such file or directory"
        )))
    );

    // Correct file
    let meta = manager
        .import_from_path(&ROOT_ID, "./test-fixtures/import.txt", false)
        .await
        .unwrap();
    assert_eq!(meta.name(), "import.txt".to_owned());

    // Wrong parent.
    let meta = manager
        .import_from_path(&meta.id(), "./test-fixtures/import.txt", false)
        .await;
    assert_eq!(meta, Err(ResourceStoreError::InvalidContainerId));

    // Duplicate name -> renaming resource.
    let meta = manager
        .import_from_path(&ROOT_ID, "./test-fixtures/import.txt", false)
        .await
        .unwrap();
    assert_eq!(meta.name(), "import(1).txt".to_owned());
}

#[async_std::test]
async fn container_size() {
    let (config, store) = prepare_test(21).await;

    let mut manager = Manager::new(config, Box::new(store)).await.unwrap();

    create_hierarchy(&mut manager).await;

    let size = manager.container_size(&ROOT_ID).await.unwrap();
    assert_eq!(size, 798);
}
