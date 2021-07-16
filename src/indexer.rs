/// Indexers for recognized mime types.
use crate::common::{BoxedReader, ObjectId, TransactionResult};
use crate::fts::Fts;
use async_std::io::{ReadExt, SeekFrom};
use async_trait::async_trait;
use futures::AsyncSeekExt;
use serde_json::Value;
use sqlx::{Sqlite, Transaction};

#[async_trait(?Send)]
pub trait Indexer {
    async fn index<'c>(
        &self,
        id: ObjectId,
        content: &mut BoxedReader,
        fts: &Fts,
        mut tx: Transaction<'c, Sqlite>,
    ) -> TransactionResult<'c>;
}

// A generic indexer for flat Json data structures.
pub struct FlatJsonIndexer {
    fields: Vec<String>,
}

impl FlatJsonIndexer {
    pub fn new(fields: &[&str]) -> Self {
        Self {
            fields: fields.iter().cloned().map(|e| e.to_owned()).collect(),
        }
    }
}

#[async_trait(?Send)]
impl Indexer for FlatJsonIndexer {
    async fn index<'c>(
        &self,
        id: ObjectId,
        content: &mut BoxedReader,
        fts: &Fts,
        mut tx: Transaction<'c, Sqlite>,
    ) -> TransactionResult<'c> {
        // 1. Read the content as json.
        content.seek(SeekFrom::Start(0)).await?;
        let mut buffer = vec![];
        content.read_to_end(&mut buffer).await?;
        let v: Value = serde_json::from_slice(&buffer)?;

        // 2. Index each available field.
        for field in &self.fields {
            if let Some(Value::String(text)) = v.get(field) {
                tx = fts.add_text(id, text, tx).await?;
            }
        }

        Ok(tx)
    }
}

// Indexer for the content of a "Places" record.
// This is a json value with the following format:
// { url: "...", title: "...", icon: "..." }
pub fn create_places_indexer() -> FlatJsonIndexer {
    FlatJsonIndexer::new(&["url", "title"])
}
