use std::str::FromStr;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Position {
    None,
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    RdbSnapshot {
        db_type: String,
        schema: String,
        tb: String,
        order_col: String,
        value: String,
    },
    RdbSnapshotFinished {
        db_type: String,
        schema: String,
        tb: String,
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
        repl_id: String,
        repl_port: u64,
        repl_offset: u64,
        now_db_id: i64,
        timestamp: String,
    },
}

impl Position {
    pub fn format_timestamp_millis(millis: i64) -> String {
        let naive_datetime = DateTime::from_timestamp_millis(millis);
        naive_datetime
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S%.3f UTC-0000")
            .to_string()
    }
}

impl ToString for Position {
    fn to_string(&self) -> String {
        json!(self).to_string()
    }
}

impl FromStr for Position {
    type Err = Error;
    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let me: Self = serde_json::from_str(str).unwrap();
        Ok(me)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_format_timestamp() {
        assert_eq!(
            "2023-03-28 07:33:48.396 UTC-0000",
            Position::format_timestamp_millis(733304028396543 / 1000 + 946_684_800 * 1000)
        );

        assert_eq!(
            "2023-03-28 05:33:47.000 UTC-0000",
            Position::format_timestamp_millis(1679981627 * 1000)
        );
    }

    #[test]
    fn test_from_str() {
        let strs = [
            r#"{"type":"None"}"#,
            r#"{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"numeric_table","order_col":"f_0","value":"127"}"#,
        ];

        for str in strs {
            let position = Position::from_str(str).unwrap();
            assert_eq!(str, &position.to_string());
        }
    }
}
