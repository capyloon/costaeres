/// Shared traits and structs.
use async_std::io::Read;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

#[derive(sqlx::Type, Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[sqlx(transparent)]
pub struct ObjectId(i64);

pub static ROOT_OBJECT_ID: ObjectId = ObjectId(0);

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for ObjectId {
    fn from(val: i64) -> Self {
        ObjectId(val)
    }
}

#[derive(sqlx::Type, Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[repr(u8)]
pub enum ObjectKind {
    Container,
    Leaf,
}

impl From<i64> for ObjectKind {
    fn from(val: i64) -> Self {
        match val {
            0 => Self::Container,
            1 => Self::Leaf,
            _ => panic!("Invalid ObjectKind value: {}", val),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct ObjectMetadata {
    id: ObjectId,
    parent: ObjectId,
    kind: ObjectKind,
    size: i64,
    name: String,
    mime_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<Vec<String>>,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
}

impl ObjectMetadata {
    pub fn new(
        id: ObjectId,
        parent: ObjectId,
        kind: ObjectKind,
        size: i64,
        name: &str,
        mime_type: &str,
        tags: Option<Vec<String>>,
    ) -> Self {
        Self {
            id,
            parent,
            kind,
            size,
            name: name.into(),
            mime_type: mime_type.into(),
            tags,
            created: Utc::now(),
            modified: Utc::now(),
        }
    }
}

impl ObjectMetadata {
    pub fn id(&self) -> ObjectId {
        self.id
    }

    pub fn parent(&self) -> ObjectId {
        self.parent
    }

    pub fn kind(&self) -> ObjectKind {
        self.kind
    }

    pub fn size(&self) -> i64 {
        self.size
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn mime_type(&self) -> String {
        self.mime_type.clone()
    }

    pub fn created(&self) -> DateTime<Utc> {
        self.created.clone()
    }

    pub fn set_created(&mut self, date: DateTime<Utc>) {
        self.created = date;
    }

    pub fn set_modified(&mut self, date: DateTime<Utc>) {
        self.modified = date;
    }

    pub fn modified(&self) -> DateTime<Utc> {
        self.modified.clone()
    }

    pub fn tags(&self) -> &Option<Vec<String>> {
        &self.tags
    }

    pub fn set_tags(&mut self, tags: Option<Vec<String>>) {
        self.tags = tags;
    }
}

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("Object Already Exists")]
    ObjectAlreadyExists,
    #[error("No Such Object")]
    NoSuchObject,
    #[error("Object Cycle Detected")]
    ObjectCycle,
    #[error("Custom Error: {0}")]
    Custom(String),
    #[error("Sqlx error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("Serde JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O Error: {0}")]
    Io(#[from] async_std::io::Error),
    #[error("Invalid Container Id")]
    InvalidContainerId,
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

impl PartialEq for ObjectStoreError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Custom(error1), Self::Custom(error2)) => error1 == error2,
            (Self::ObjectAlreadyExists, Self::ObjectAlreadyExists)
            | (Self::NoSuchObject, Self::NoSuchObject)
            | (Self::ObjectCycle, Self::ObjectCycle)
            | (Self::Sql(_), Self::Sql(_))
            | (Self::Json(_), Self::Json(_))
            | (Self::Io(_), Self::Io(_))
            | (Self::InvalidContainerId, Self::InvalidContainerId)
            | (Self::Bincode(_), Self::Bincode(_)) => true,
            _ => false,
        }
    }
}

pub type BoxedReader = Box<dyn Read + Unpin>;

// Operations needed for an object store.
// These are all async operations.
#[async_trait(?Send)]
pub trait ObjectStore {
    async fn create(
        &self,
        metadata: &ObjectMetadata,
        content: BoxedReader,
    ) -> Result<(), ObjectStoreError>;

    async fn update(
        &self,
        metadata: &ObjectMetadata,
        content: BoxedReader,
    ) -> Result<(), ObjectStoreError>;

    async fn update_content_from_slice(
        &self,
        id: ObjectId,
        content: &[u8],
    ) -> Result<(), ObjectStoreError>;

    async fn delete(&self, id: ObjectId) -> Result<(), ObjectStoreError>;

    async fn get_metadata(&self, id: ObjectId) -> Result<ObjectMetadata, ObjectStoreError>;

    async fn get_content(&self, id: ObjectId) -> Result<BoxedReader, ObjectStoreError>;

    async fn get_full(
        &self,
        id: ObjectId,
    ) -> Result<(ObjectMetadata, BoxedReader), ObjectStoreError>;
}

// Operations needed for an object manager.
#[async_trait(?Send)]
pub trait ObjectManager {
    async fn create(
        &self,
        metadata: &ObjectMetadata,
        content: BoxedReader,
    ) -> Result<(), ObjectStoreError>;

    async fn update(
        &self,
        metadata: &ObjectMetadata,
        content: BoxedReader,
    ) -> Result<(), ObjectStoreError>;

    async fn delete(&self, id: ObjectId) -> Result<(), ObjectStoreError>;

    async fn get_metadata(&self, id: ObjectId) -> Result<ObjectMetadata, ObjectStoreError>;

    // Returns the metadata and content for a leaf object.
    async fn get_leaf(
        &self,
        id: ObjectId,
    ) -> Result<(ObjectMetadata, BoxedReader), ObjectStoreError>;

    // Returns the metadata for the container, and the list of metadata objects for its children, if any.
    async fn get_container(
        &self,
        id: ObjectId,
    ) -> Result<(ObjectMetadata, Vec<ObjectMetadata>), ObjectStoreError>;
}
