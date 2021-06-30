/// Configuration file definition.
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub db_path: String,
    pub data_dir: String,
}
