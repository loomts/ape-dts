use crate::meta::db_enums::DbType;

#[derive(Clone)]
pub enum ExtractorConfig {
    BasicStruct {
        url: String,
        db_type: DbType,
    },

    MysqlSnapshot {
        url: String,
        db: String,
        tb: String,
    },

    MysqlCdc {
        url: String,
        binlog_filename: String,
        binlog_position: u32,
        server_id: u64,
    },

    MysqlCheck {
        url: String,
        check_log_dir: String,
        batch_size: usize,
    },

    PgSnapshot {
        url: String,
        db: String,
        tb: String,
    },

    PgCdc {
        url: String,
        slot_name: String,
        start_lsn: String,
        heartbeat_interval_secs: u64,
    },

    PgCheck {
        url: String,
        check_log_dir: String,
        batch_size: usize,
    },
}
