use crate::redis::redis_entry::RedisEntry;

use super::{ddl_data::DdlData, row_data::RowData};

#[derive(Debug, Clone)]
pub enum DtData {
    Ddl { ddl_data: DdlData },
    Dml { row_data: RowData },
    Commit { xid: String, position: String },
    Redis { entry: RedisEntry },
}

impl DtData {
    pub fn is_ddl(&self) -> bool {
        matches!(self, DtData::Ddl { .. })
    }

    pub fn is_raw(&self) -> bool {
        matches!(self, DtData::Redis { .. })
    }
}
