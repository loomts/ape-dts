use std::collections::HashMap;

use super::mysql_col_meta::MysqlColMeta;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MysqlTbMeta {
    pub db: String,
    pub tb: String,
    pub cols: Vec<String>,
    pub col_meta_map: HashMap<String, MysqlColMeta>,
    pub key_map: HashMap<String, Vec<String>>,
    pub order_col: Option<String>,
    pub partition_col: String,
    pub where_cols: Vec<String>,
}
