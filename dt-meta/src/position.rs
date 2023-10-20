use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Position {
    None,
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    RdbSnapshot {
        order_col: String,
        value: String,
    },
    MysqlCdc {
        server_id: String,
        binlog_filename: String,
        next_event_position: u32,
        timestamp: String,
    },
    PgCdc {
        lsn: String,
        timestamp: String,
    },
    MongoCdc {
        resume_token: String,
        operation_time: u32,
        timestamp: String,
    },
    Redis {
        run_id: String,
        repl_offset: u64,
        now_db_id: i64,
    },
}

impl Position {
    pub fn to_string(&self) -> String {
        json!(self).to_string()
    }

    pub fn from_str(str: &str) -> Position {
        serde_json::from_str(str).unwrap()
    }
}
