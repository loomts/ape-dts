use std::str::FromStr;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl FromStr for Position {
    type Err = Error;
    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let me: Self = serde_json::from_str(str).unwrap();
        Ok(me)
    }
}

impl Position {
    pub fn from_log(log: &str) -> Position {
        // 2024-03-29 07:02:24.463776 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col":"f_0","value":"9"}
        // 2024-04-01 03:25:18.701725 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk"}
        if log.trim().is_empty() {
            return Position::None;
        }

        let error = format!("invalid position log: {}", log);
        let left = log.find('{').expect(&error);
        let right = log.rfind('}').expect(&error);
        let position_log = &log[left..=right];
        Position::from_str(position_log).expect(&error)
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

    #[test]
    fn test_from_log() {
        let log1 = r#"2024-04-01 03:25:18.701725 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk"}"#;
        let log2 = r#"2024-03-29 07:02:24.463776 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col":"f_0","value":"9"}"#;

        if let Position::RdbSnapshotFinished {
            db_type,
            schema,
            tb,
        } = Position::from_log(log1)
        {
            assert_eq!(db_type, "mysql");
            assert_eq!(schema, "test_db_1");
            assert_eq!(tb, "one_pk_no_uk");
        } else {
            assert!(false)
        }

        if let Position::RdbSnapshot {
            db_type,
            schema,
            tb,
            order_col,
            value,
        } = Position::from_log(log2)
        {
            assert_eq!(db_type, "mysql");
            assert_eq!(schema, "test_db_1");
            assert_eq!(tb, "one_pk_no_uk");
            assert_eq!(order_col, "f_0");
            assert_eq!(value, "9");
        } else {
            assert!(false)
        }
    }
}
