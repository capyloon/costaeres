/// A naive implementation of Full Text Search.
/// The goal is to provide substring matching to object names and tags.
///
/// Using a simple SQlite table (ResourceId, ngram) which makes it easy to
/// manage object removal at the expense of disk space usage and query performance.
/// TODO: switch to a Key Value store (eg. Sled) instead, or a fts engine like Sonic.
use crate::common::{IdFrec, ResourceId, ResourceStoreError, TransactionResult};
use crate::timer::Timer;
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
        id: &ResourceId,
        text: &str,
        mut tx: Transaction<'c, Sqlite>,
    ) -> TransactionResult<'c> {
        let ngrams = ngrams(text, self.max_substring_len);
        let _timer = Timer::start(&format!("Fts::add_text {} ngrams", ngrams.len()));

        let id = id.clone();

        let mut knowns: HashSet<String> = HashSet::new();
        sqlx::query!(
            "SELECT ngram0, ngram1, ngram2, ngram3, ngram4,
                    ngram5, ngram6, ngram7, ngram8, ngram9 FROM fts WHERE id = ?",
            id
        )
        .map(|r| {
            macro_rules! insert_ngram {
                ($num:tt) => {
                    if !r.$num.is_empty() {
                        knowns.insert(r.$num.clone());
                    }
                };
            }
            insert_ngram!(ngram0);
            insert_ngram!(ngram1);
            insert_ngram!(ngram2);
            insert_ngram!(ngram3);
            insert_ngram!(ngram4);
            insert_ngram!(ngram5);
            insert_ngram!(ngram6);
            insert_ngram!(ngram7);
            insert_ngram!(ngram8);
            insert_ngram!(ngram9);
        })
        .fetch_all(&mut tx)
        .await?;

        let to_insert: Vec<String> = ngrams
            .iter()
            .filter(|item| !knowns.contains(*item))
            .cloned()
            .collect();

        for chunk in to_insert.chunks(10) {
            let empty = &String::new();
            let mut iter = chunk.iter();
            let chunk0 = iter.next().unwrap_or(empty);
            let chunk1 = iter.next().unwrap_or(empty);
            let chunk2 = iter.next().unwrap_or(empty);
            let chunk3 = iter.next().unwrap_or(empty);
            let chunk4 = iter.next().unwrap_or(empty);
            let chunk5 = iter.next().unwrap_or(empty);
            let chunk6 = iter.next().unwrap_or(empty);
            let chunk7 = iter.next().unwrap_or(empty);
            let chunk8 = iter.next().unwrap_or(empty);
            let chunk9 = iter.next().unwrap_or(empty);

            sqlx::query!(
                "INSERT OR IGNORE INTO fts ( id, ngram0, ngram1, ngram2, ngram3, ngram4, ngram5, ngram6, ngram7, ngram8, ngram9 )
                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )",
                id,
                chunk0,
                chunk1,
                chunk2,
                chunk3,
                chunk4,
                chunk5,
                chunk6,
                chunk7,
                chunk8,
                chunk9
            )
            .execute(&mut tx)
            .await?;
        }

        Ok(tx)
    }

    // Return objects that have a match for all tokens, ordered by frecency.
    pub async fn search(
        &self,
        text: &str,
        tag: Option<String>,
    ) -> Result<Vec<IdFrec>, ResourceStoreError> {
        let _timer = Timer::start(&format!("Fts::search {} {:?}", text, tag));

        let mut tx = self.db_pool.begin().await?;
        // Map ResourceId -> (ngram matches, frecency)
        let mut res: HashMap<ResourceId, (usize, u32)> = HashMap::new();

        let words = preprocess_text(text);

        let len = words.len();
        for mut word in words {
            if word.len() > self.max_substring_len {
                word = word[0..self.max_substring_len].to_owned();
            }

            let records: Vec<IdFrec> = match tag {
                None => sqlx::query_as(
                    r#"SELECT resources.id, frecency(resources.scorer) AS frecency FROM resources
                        LEFT JOIN fts
                        WHERE fts.id = resources.id
                        AND (fts.ngram0 = ? OR fts.ngram1 = ? OR fts.ngram2 = ? OR fts.ngram3 = ? OR fts.ngram4 = ?
                          OR fts.ngram5 = ? OR fts.ngram6 = ? OR fts.ngram7 = ? OR fts.ngram8 = ? OR fts.ngram9 = ? )"#,
                )
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .fetch_all(&mut tx)
                .await?,
                Some(ref tag) => sqlx::query_as(
                    r#"SELECT resources.id, frecency(resources.scorer) AS frecency FROM resources
                        LEFT JOIN fts, tags
                        WHERE tags.tag = ?
                        AND fts.id = resources.id AND tags.id = resources.id
                        AND (fts.ngram0 = ? OR fts.ngram1 = ? OR fts.ngram2 = ? OR fts.ngram3 = ? OR fts.ngram4 = ?
                          OR fts.ngram5 = ? OR fts.ngram6 = ? OR fts.ngram7 = ? OR fts.ngram8 = ? OR fts.ngram9 = ? )"#,
                )
                .bind(tag)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .bind(&word)
                .fetch_all(&mut tx)
                .await?,
            };
            records.iter().for_each(|r| {
                res.entry(r.id.clone())
                    .and_modify(|e| (*e).0 += 1)
                    .or_insert((1, r.frecency));
            });
        }

        let mut matches: Vec<IdFrec> = res
            .iter()
            .filter_map(|item| {
                if item.1 .0 == len {
                    Some(IdFrec::new(item.0, item.1 .1))
                } else {
                    None
                }
            })
            .collect();
        matches.sort_by(|a, b| b.frecency.partial_cmp(&a.frecency).unwrap());
        Ok(matches)
    }
}

// TODO: maybe switch to https://docs.rs/secular/1.0.1/secular/
fn remove_diacritics(input: &str) -> String {
    let v = input.chars();
    let mut split_string: Vec<String> = Vec::new();
    for c in v {
        split_string.push(String::from(c));
    }
    split_string
        .iter()
        .map(|c| match c.as_ref() {
            "À" | "Á" | "Â" | "Ã" | "Ä" | "Å" | "Æ" => "A",
            "Þ" => "B",
            "Ç" | "Č" => "C",
            "Ď" | "Ð" => "D",
            "Ě" | "È" | "É" | "Ê" | "Ë" => "E",
            "Ƒ" => "F",
            "Ì" | "Í" | "Î" | "Ï" => "I",
            "Ň" | "Ñ" => "N",
            "Ò" | "Ó" | "Ô" | "Õ" | "Ö" | "Ø" => "O",
            "Ř" => "R",
            "ß" => "ss",
            "Š" => "S",
            "Ť" => "T",
            "Ů" | "Ù" | "Ú" | "Û" | "Ü" => "U",
            "Ý" => "Y",
            "Ž" => "Z",

            "à" | "á" | "â" | "ã" | "ä" | "å" | "æ" => "a",
            "þ" => "b",
            "ç" | "č" => "c",
            "ď" | "ð" => "d",
            "ě" | "è" | "é" | "ê" | "ë" => "e",
            "ƒ" => "f",
            "ì" | "í" | "î" | "ï" => "i",
            "ñ" | "ň" => "n",
            "ò" | "ó" | "ô" | "õ" | "ö" | "ø" => "o",
            "ř" => "r",
            "š" => "s",
            "ť" => "t",
            "ů" | "ù" | "ú" | "û" | "ü" => "u",
            "ý" | "ÿ" => "y",
            "ž" => "z",

            _ => c,
        })
        .collect()
}

fn preprocess_text(text: &str) -> Vec<String> {
    // Turn the text into lowercase, convert to ascii and split tokens as whitespace separated.
    let lowercase = remove_diacritics(text).to_lowercase();
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
