use crate::meta::db_enum::DatabaseType;

#[derive(Clone)]
pub enum ExtractorConfig {
    BasicStruct {
        url: String,
        db_type: DatabaseType,
    },

    MysqlSnapshot {
        url: String,
        do_tb: String,
    },

    MysqlCdc {
        url: String,
        binlog_filename: String,
        binlog_position: u32,
        server_id: u64,
    },

    PgSnapshot {
        url: String,
        do_tb: String,
    },

    PgCdc {
        url: String,
        slot_name: String,
        start_lsn: String,
    },
}
