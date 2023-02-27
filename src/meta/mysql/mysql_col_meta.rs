use super::mysql_col_type::MysqlColType;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MysqlColMeta {
    pub name: String,
    pub typee: MysqlColType,
}
