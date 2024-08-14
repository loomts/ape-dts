use std::str::FromStr;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::meta::position::Position;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct S3FileMeta {
    pub schema: String,
    pub tb: String,
    pub insert_only: bool,
    pub meta_file_name: String,
    pub data_file_name: String,
    pub data_size: usize,
    pub row_count: usize,
    pub last_position: Position,
    pub sequencer_id: i64,
    pub push_epoch: i64,
    pub push_sequence: u64,
}

impl std::fmt::Display for S3FileMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl FromStr for S3FileMeta {
    type Err = anyhow::Error;
    fn from_str(str: &str) -> anyhow::Result<Self, anyhow::Error> {
        let me: Self = serde_json::from_str(str)
            .with_context(|| format!("invalid s3 file meta str: [{}]", str))?;
        Ok(me)
    }
}
