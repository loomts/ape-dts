use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{kafka::kafka_message::KafkaMessage, redis::redis_entry::RedisEntry};

use super::{ddl_data::DdlData, row_data::RowData};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DtData {
    Ddl {
        ddl_data: DdlData,
    },
    Dml {
        row_data: RowData,
    },
    Commit {
        xid: String,
        position: String,
    },
    #[serde(skip)]
    Redis {
        entry: RedisEntry,
    },
    #[serde(skip)]
    Kafka {
        message: KafkaMessage,
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
