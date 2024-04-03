use std::collections::HashMap;

use crate::meta::rdb_tb_meta::RdbTbMeta;

use super::pg_col_type::PgColType;

#[derive(Debug, Clone)]
pub struct PgTbMeta {
    pub basic: RdbTbMeta,
    pub oid: i32,
    pub col_type_map: HashMap<String, PgColType>,
}
