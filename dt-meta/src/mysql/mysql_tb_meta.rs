use std::collections::HashMap;

use crate::rdb_tb_meta::RdbTbMeta;

use super::mysql_col_type::MysqlColType;

#[derive(Debug, Clone)]
pub struct MysqlTbMeta {
    pub basic: RdbTbMeta,
    pub col_type_map: HashMap<String, MysqlColType>,
}
