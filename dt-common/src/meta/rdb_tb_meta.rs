use std::collections::HashMap;

use serde::Serialize;

use crate::meta::foreign_key::ForeignKey;

#[derive(Debug, Clone, Default, Serialize)]
pub struct RdbTbMeta {
    pub schema: String,
    pub tb: String,
    pub cols: Vec<String>,
    pub key_map: HashMap<String, Vec<String>>,
    pub order_col: Option<String>,
    pub partition_col: String,
    pub id_cols: Vec<String>,
    pub foreign_keys: Vec<ForeignKey>,
}
