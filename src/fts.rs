/// A naive implementation of Full Text Search.
/// The goal is to provide substring matching to object names and tags.
///
/// Using a simple SQlite table (ObjectId, ngram) which makes it easy to
/// manage object removal at the expense of disk space usage and query performance.
/// TODO: switch to a Key Value store (eg. Sled) instead, or a fts engine like Sonic.
use crate::common::{ObjectId, ObjectStoreError};
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::collections::{HashMap, HashSet};

pub struct Fts {
    db_pool: SqlitePool,
    max_substring_len: usize,
}

impl Fts {
    pub fn new(pool: &SqlitePool, max_substring_len: usize) -> Self {
        Self {
            db_pool: pool.clone(),
            max_substring_len,
        }
    }

    pub async fn add_text<'c>(
        &self,
        id: ObjectId,
        text: &str,
        mut tx: Transaction<'c, Sqlite>,
    ) -> Result<Transaction<'c, Sqlite>, ObjectStoreError> {
        let ngrams = ngrams(text, self.max_substring_len);
        // let mut tx = self.db_pool.begin().await?;
        for ngram in ngrams {
            sqlx::query!("INSERT INTO fts ( id, ngram ) VALUES ( ?1, ?2 )", id, ngram)
                .execute(&mut tx)
                .await?;
        }

        Ok(tx)
    }

    // Return objects that have a match for all tokens
    pub async fn search(&self, text: &str) -> Result<Vec<(ObjectId, u32)>, ObjectStoreError> {
        let mut tx = self.db_pool.begin().await?;
        // Map ObjectId -> (ngram matches, frecency)
        let mut res: HashMap<ObjectId, (usize, u32)> = HashMap::new();

        let words = preprocess_text(text);

        let len = words.len();
        for mut word in words {
            if word.len() > self.max_substring_len {
                word = word[0..self.max_substring_len].to_owned();
            }

            sqlx::query!(
                r#"SELECT objects.id, objects.frecency FROM objects
            LEFT JOIN fts
            WHERE fts.ngram = ? and fts.id = objects.id"#,
                word
            )
            // sqlx::query!("SELECT id FROM fts WHERE ngram = ?", word)
            .fetch_all(&mut tx)
            .await?
            .iter()
            .for_each(|r| {
                res.entry(r.id.into())
                    .and_modify(|e| (*e).0 += 1)
                    .or_insert((1, r.frecency as _));
            });
        }

        let mut matches: Vec<(ObjectId, u32)> = res
            .iter()
            .filter_map(|item| {
                if item.1 .0 == len {
                    Some((*item.0, item.1 .1))
                } else {
                    None
                }
            })
            .collect();
        matches.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        Ok(matches)
    }
}

fn preprocess_text(text: &str) -> Vec<String> {
    // Turn the text into lowercase and split tokens as whitespace separated.
    let lowercase = text.to_lowercase();
    let words = lowercase.split_whitespace();
    words.into_iter().map(|s| s.trim().to_owned()).collect()
}

// Returns the list of substrings that will match the given text.
fn ngrams(text: &str, max_substring_len: usize) -> Vec<String> {
    let mut res = vec![];
    let mut seen = HashSet::new();

    for word in preprocess_text(text) {
        let max_len = word.len();

        for len in 1..=std::cmp::min(max_substring_len, max_len) {
            let max = max_len - len;
            for pos in 0..=max {
                if pos + len > max_len {
                    break;
                }
                if let Some(substr) = word.get(pos..pos + len) {
                    if !seen.contains(substr) {
                        seen.insert(substr.to_owned());
                        res.push(substr.to_owned());
                    }
                } else {
                    break;
                }
            }
        }
    }

    res
}

#[test]
fn find_ngrams() {
    let res = ngrams("Hello World", 3);
    assert_eq!(res.len(), 21);

    let res = ngrams("https://en.wikipedia.org/wiki/Freediving", 10);
    // println!("{:?}", res);
    assert_eq!(res.len(), 325);

    let res = ngrams("0", 5);
    println!("{:?}", res);
    assert_eq!(res.len(), 1);

    let res = ngrams("child #17", 5);
    println!("{:?}", res);
    assert_eq!(res.len(), 21);
}
