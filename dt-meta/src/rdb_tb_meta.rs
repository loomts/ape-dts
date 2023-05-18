use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RdbTbMeta {
    pub schema: String,
    pub tb: String,
    pub cols: Vec<String>,
    pub key_map: HashMap<String, Vec<String>>,
    pub order_col: Option<String>,
    pub partition_col: String,
    pub id_cols: Vec<String>,
}
