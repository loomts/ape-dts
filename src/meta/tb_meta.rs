use std::collections::HashMap;

use super::col_meta::ColMeta;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TbMeta {
    pub db: String,
    pub tb: String,
    pub cols: Vec<String>,
    pub col_meta_map: HashMap<String, ColMeta>,
    pub key_map: HashMap<String, Vec<String>>,
    pub order_col: Option<String>,
    pub where_cols: Vec<String>,
}
