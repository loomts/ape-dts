use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;

use crate::meta::rdb_tb_meta::RdbTbMeta;

use super::duckdb_col_type::DuckdbColType;

#[derive(Debug, Clone, Serialize)]
pub struct DuckdbTbMeta {
    pub basic: RdbTbMeta,
    pub col_type_map: HashMap<String, DuckdbColType>,
}

impl std::fmt::Display for DuckdbTbMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}
