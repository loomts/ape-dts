use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{position::Position, redis::redis_entry::RedisEntry};

use super::{ddl_data::DdlData, row_data::RowData};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DtItem {
    pub dt_data: DtData,
    pub position: Position,
}

impl DtItem {
    pub fn is_ddl(&self) -> bool {
        self.dt_data.is_ddl()
    }

    pub fn get_data_malloc_size(&self) -> usize {
        match &self.dt_data {
            DtData::Dml { row_data } => row_data.get_data_malloc_size(),
            DtData::Redis { entry } => entry.get_data_malloc_size(),
            // ignore other item types
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DtData {
    Ddl {
        ddl_data: DdlData,
    },
    Dml {
        row_data: RowData,
    },
    Begin {},
    Commit {
        xid: String,
    },
    #[serde(skip)]
    Redis {
        entry: RedisEntry,
    },
}

impl DtData {
    pub fn is_ddl(&self) -> bool {
        matches!(self, DtData::Ddl { .. })
    }

    pub fn to_string(&self) -> String {
        json!(self).to_string()
    }
}
