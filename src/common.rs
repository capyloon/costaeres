/// Shared traits and structs.
use crate::scorer::{Scorer, VisitEntry};
use async_std::io::{Read, Seek};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use sqlx::{sqlite::SqliteRow, FromRow, Row, Sqlite, Transaction};
use std::fmt;
use thiserror::Error;

#[derive(
    sqlx::Type, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Readable, Writable,
)]
#[sqlx(transparent)]
pub struct ResourceId(String);

static ROOT_ID_STR: &str = "9e48b88d-4ab5-496b-ad7f-9ecc685128db";

impl ResourceId {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub fn is_root(&self) -> bool {
        self.0 == ROOT_ID_STR
    }
}

impl fmt::Display for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

lazy_static! {
    pub static ref ROOT_ID: ResourceId = ResourceId(ROOT_ID_STR.into());
}

pub type TransactionResult<'c> = Result<Transaction<'c, Sqlite>, ResourceStoreError>;

// Only useful for tests
impl From<i32> for ResourceId {
    fn from(val: i32) -> ResourceId {
        ResourceId::from(format!("id-{}", val))
    }
}

impl From<String> for ResourceId {
    fn from(val: String) -> Self {
        ResourceId(val)
    }
}

impl From<ResourceId> for String {
    fn from(val: ResourceId) -> String {
        val.0
    }
}

// Extracts a ResourceId from the first column of a row.
impl<'r> FromRow<'r, SqliteRow> for ResourceId {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(row.get::<String, usize>(0).into())
    }
}

#[derive(sqlx::FromRow, PartialEq, Debug)]
pub struct IdFrec {
    pub id: ResourceId,
    pub frecency: u32,
}

impl IdFrec {
    pub fn new(id: &ResourceId, frecency: u32) -> Self {
        Self {
            id: id.clone(),
            frecency,
        }
    }
}

#[derive(sqlx::Type, Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[repr(u8)]
pub enum ResourceKind {
    Container,
    Leaf,
}

impl From<i64> for ResourceKind {
    fn from(val: i64) -> Self {
        match val {
            0 => Self::Container,
            1 => Self::Leaf,
            _ => panic!("Invalid ResourceKind value: {}", val),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Variant {
    name: String,
    mime_type: String,
    size: u32,
}

impl Variant {
    pub fn new(name: &str, mime_type: &str, size: u32) -> Self {
        Self {
            name: name.into(),
            mime_type: mime_type.into(),
            size,
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = name.into();
    }

    pub fn mime_type(&self) -> String {
        self.mime_type.clone()
    }

    pub fn set_mime_type(&mut self, mime_type: &str) {
        self.mime_type = mime_type.into();
    }

    pub fn size(&self) -> u32 {
        self.size
    }

    pub fn set_size(&mut self, size: u32) {
        self.size = size;
    }
}

pub struct VariantContent(pub Variant, pub BoxedReader);

impl VariantContent {
    pub fn new(variant: Variant, reader: BoxedReader) -> Self {
        VariantContent(variant, reader)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ResourceMetadata {
    id: ResourceId,
    parent: ResourceId,
    kind: ResourceKind,
    name: String,
    tags: Vec<String>,
    variants: Vec<Variant>,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    scorer: Scorer,
}

impl ResourceMetadata {
    pub fn new(
        id: &ResourceId,
        parent: &ResourceId,
        kind: ResourceKind,
        name: &str,
        tags: Vec<String>,
        variants: Vec<Variant>,
    ) -> Self {
        Self {
            id: id.clone(),
            parent: parent.clone(),
            kind,
            name: name.into(),
            tags,
            variants,
            created: Utc::now(),
            modified: Utc::now(),
            scorer: Scorer::default(),
        }
    }

    pub fn has_variant(&self, name: &str) -> bool {
        self.variants.iter().any(|item| item.name() == name)
    }

    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|item| item == tag)
    }

    pub fn id(&self) -> ResourceId {
        self.id.clone()
    }

    pub fn parent(&self) -> ResourceId {
        self.parent.clone()
    }

    pub fn kind(&self) -> ResourceKind {
        self.kind
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn scorer(&self) -> &Scorer {
        &self.scorer
    }

    pub fn set_scorer(&mut self, scorer: &Scorer) {
        self.scorer = scorer.clone();
    }

    pub fn update_scorer(&mut self, entry: &VisitEntry) {
        self.scorer.add(entry);
    }

    // Returns a bincode representation of the score, suitable to store in the DB.
    pub fn db_scorer(&self) -> Vec<u8> {
        self.scorer.as_binary()
    }

    // Set the scorer using the db serialized representation.
    pub fn set_scorer_from_db(&mut self, serialized: &[u8]) {
        self.scorer = Scorer::from_binary(serialized)
    }

    pub fn created(&self) -> DateTime<Utc> {
        self.created
    }

    pub fn set_created(&mut self, date: DateTime<Utc>) {
        self.created = date;
    }

    pub fn modified(&self) -> DateTime<Utc> {
        self.modified
    }

    pub fn set_modified(&mut self, date: DateTime<Utc>) {
        self.modified = date;
    }

    pub fn modify_now(&mut self) {
        self.modified = Utc::now();
    }

    pub fn tags(&self) -> &Vec<String> {
        &self.tags
    }

    pub fn set_tags(&mut self, tags: Vec<String>) {
        self.tags = tags;
    }

    pub fn variants(&self) -> &Vec<Variant> {
        &self.variants
    }

    pub fn set_variants(&mut self, variants: Vec<Variant>) {
        self.variants = variants;
    }

    pub fn add_variant(&mut self, variant: Variant) {
        if !self.has_variant(&variant.name()) {
            self.variants.push(variant);
        }
    }

    pub fn delete_variant(&mut self, name: &str) {
        self.variants = self
            .variants
            .iter()
            .filter_map(|item| {
                if item.name() != name {
                    let v: Variant = item.clone();
                    Some(v)
                } else {
                    None
                }
            })
            .collect();
    }

    pub fn mime_type_for_variant(&self, variant_name: &str) -> Option<String> {
        for variant in &self.variants {
            if variant.name() == variant_name {
                return Some(variant.mime_type());
            }
        }

        None
    }
}

#[derive(Debug, Error)]
pub enum ResourceStoreError {
    #[error("Resource Already Exists")]
    ResourceAlreadyExists,
    #[error("No Such Resource")]
    NoSuchResource,
    #[error("Resource Cycle Detected")]
    ResourceCycle,
    #[error("Invalid Variant For This Resource: {0}")]
    InvalidVariant(String),
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
    #[error("Speedy error: {0}")]
    Speedy(#[from] speedy::Error),
}

impl PartialEq for ResourceStoreError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Custom(error1), Self::Custom(error2)) => error1 == error2,
            (Self::ResourceAlreadyExists, Self::ResourceAlreadyExists)
            | (Self::NoSuchResource, Self::NoSuchResource)
            | (Self::ResourceCycle, Self::ResourceCycle)
            | (Self::Sql(_), Self::Sql(_))
            | (Self::Json(_), Self::Json(_))
            | (Self::Io(_), Self::Io(_))
            | (Self::InvalidContainerId, Self::InvalidContainerId)
            | (Self::Speedy(_), Self::Speedy(_)) => true,
            (Self::InvalidVariant(v1), Self::InvalidVariant(v2)) => v1 == v2,
            _ => false,
        }
    }
}

pub trait ReaderTrait: Read + Seek {}

// Generic implementation.
impl<T: Seek + Unpin + Read + ?Sized> ReaderTrait for Box<T> {}

// Special case for files.
impl ReaderTrait for async_std::fs::File {}

// Special case for slices.
impl ReaderTrait for async_std::io::Cursor<&[u8]> {}

pub type BoxedReader = Box<dyn ReaderTrait + Unpin>;

/// Operations needed for a resource store.
#[async_trait(?Send)]
pub trait ResourceStore {
    /// Creates a new resource with some metadata and an initial variant.
    /// This function will fail if a resource with the same id already exists.
    /// The variant passed must be in the metadata variant list.
    async fn create(
        &self,
        metadata: &ResourceMetadata,
        content: Option<VariantContent>,
    ) -> Result<(), ResourceStoreError>;

    /// Updates the metadata and variant for a resource.
    /// The variant passed must be in the metadata variant list.
    async fn update(
        &self,
        metadata: &ResourceMetadata,
        content: Option<VariantContent>,
    ) -> Result<(), ResourceStoreError>;

    /// Helper method to update the default variant using
    /// a slice as input.
    /// This is an optimization for container content.
    async fn update_default_variant_from_slice(
        &self,
        id: &ResourceId,
        content: &[u8],
    ) -> Result<(), ResourceStoreError>;

    /// Fully deletes a resource: metadata and all variants.
    async fn delete(&self, id: &ResourceId) -> Result<(), ResourceStoreError>;

    /// Deletes a single variant for this resource.
    async fn delete_variant(
        &self,
        id: &ResourceId,
        variant: &str,
    ) -> Result<(), ResourceStoreError>;

    /// Fetches the metadata for a resource.
    async fn get_metadata(&self, id: &ResourceId) -> Result<ResourceMetadata, ResourceStoreError>;

    /// Fetches the content for a resource's variant.
    async fn get_variant(
        &self,
        id: &ResourceId,
        variant: &str,
    ) -> Result<BoxedReader, ResourceStoreError>;

    /// Fetches both the metadata and the given variant for a resource.
    async fn get_full(
        &self,
        id: &ResourceId,
        variant: &str,
    ) -> Result<(ResourceMetadata, BoxedReader), ResourceStoreError>;
}

/// A trait to implement that makes it possible to assign non-default
/// names to resource files:
/// - meta files.
/// - variant files.
/// This is useful to make it harder to learn about the resource set
/// based on file names only.
pub trait ResourceNameProvider: Sync + Send {
    /// Provides the name for the metadata file.
    fn metadata_name(&self, id: &ResourceId) -> String;

    // Provides the name for a variant file.
    fn variant_name(&self, id: &ResourceId, variant: &str) -> String;
}

pub struct DefaultResourceNameProvider;

unsafe impl Sync for DefaultResourceNameProvider {}
unsafe impl Send for DefaultResourceNameProvider {}

impl ResourceNameProvider for DefaultResourceNameProvider {
    fn metadata_name(&self, id: &ResourceId) -> String {
        format!("{}.meta", id)
    }

    fn variant_name(&self, id: &ResourceId, variant: &str) -> String {
        format!("{}.variant.{}", id, variant)
    }
}

/// A trait to implement in order to transform data stored as it is
/// read and written.
pub trait ResourceTransformer: Sync + Send {
    /// Creates a wrapper around a reader use to write a resource to storage.
    fn transform_to(&self, source: BoxedReader) -> BoxedReader;

    /// Creates a wrapper around a reader use to read a resource from storage.
    fn transform_from(&self, source: BoxedReader) -> BoxedReader;

    /// Transforms an array that will be written.
    fn transform_array_to(&self, source: &[u8]) -> Vec<u8>;

    /// Transforms an array that was read.
    fn transform_array_from(&self, source: &[u8]) -> Vec<u8>;
}

pub struct IdentityTransformer;

unsafe impl Sync for IdentityTransformer {}
unsafe impl Send for IdentityTransformer {}

impl ResourceTransformer for IdentityTransformer {
    fn transform_to(&self, source: BoxedReader) -> BoxedReader {
        source
    }

    fn transform_from(&self, source: BoxedReader) -> BoxedReader {
        source
    }

    fn transform_array_to(&self, source: &[u8]) -> Vec<u8> {
        source.to_vec()
    }

    fn transform_array_from(&self, source: &[u8]) -> Vec<u8> {
        source.to_vec()
    }
}
