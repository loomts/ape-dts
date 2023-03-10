use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::pg_col_type::PgColType;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PgTbMeta {
    pub schema: String,
    pub tb: String,
    pub oid: i32,
    pub cols: Vec<String>,
    pub col_type_map: HashMap<String, PgColType>,
    pub key_map: HashMap<String, Vec<String>>,
    pub order_col: Option<String>,
    pub partition_col: String,
    pub where_cols: Vec<String>,
}
