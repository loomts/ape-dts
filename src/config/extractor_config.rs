#[derive(Clone)]
pub enum ExtractorConfig {
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
