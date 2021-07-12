use async_std::fs;
use costaeres::common::*;
use costaeres::file_store::*;

static CONTENT: [u8; 100] = [0; 100];

#[async_std::test]
async fn file_store() {
    let _ = fs::remove_dir_all("./test-content/0").await;
    let _ = fs::create_dir_all("./test-content/0").await;

    let store = FileStore::new("./test-content/0").await.unwrap();

    // Starting with no content.
    let res = store.get_full(0.into()).await.err();
    assert_eq!(res, Some(ObjectStoreError::NoSuchObject));

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

    let res = store.create(&meta, Box::new(&CONTENT[..])).await.ok();
    assert_eq!(res, Some(()));

    // Now check that we can get it.
    let res = store.get_full(0.into()).await.ok().unwrap().0;
    assert_eq!(res.id(), 0.into());
    assert_eq!(&res.name(), "object 0");

    // Check we can't add another object with the same id.
    let res = store.create(&meta, Box::new(&CONTENT[..])).await.err();
    assert_eq!(res, Some(ObjectStoreError::ObjectAlreadyExists));

    // Update the object.
    let meta = ObjectMetadata::new(
        0.into(),
        0.into(),
        ObjectKind::Leaf,
        10,
        "object 0 updated",
        "text/plain",
        Some(vec!["one".into(), "two".into()]),
    );

    let _ = store.update(&meta, Box::new(&CONTENT[..])).await.unwrap();

    let res = store.get_full(0.into()).await.ok().unwrap().0;
    assert_eq!(res.id(), 0.into());
    assert_eq!(&res.name(), "object 0 updated");

    // Now delete this object.
    let _ = store.delete(0.into()).await.ok().unwrap();

    // And check we can't get it anymore.
    let res = store.get_full(0.into()).await.err();
    assert_eq!(res, Some(ObjectStoreError::NoSuchObject));
}
