use crate::meta::db_enums::DbType;

#[derive(Clone)]
pub enum ExtractorConfig {
    MysqlBasic {
        url: String,
        db: String,
    },

    PgBasic {
        url: String,
        db: String,
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

    MongoSnapshot {
        url: String,
        db: String,
        tb: String,
    },

    MongoCdc {
        url: String,
        resume_token: String,
    },
}

impl ExtractorConfig {
    pub fn get_db_type(&self) -> DbType {
        match self {
            Self::MysqlBasic { .. }
            | Self::MysqlSnapshot { .. }
            | Self::MysqlCdc { .. }
            | Self::MysqlCheck { .. } => DbType::Mysql,

            Self::PgBasic { .. }
            | Self::PgSnapshot { .. }
            | Self::PgCdc { .. }
            | Self::PgCheck { .. } => DbType::Pg,

            Self::MongoSnapshot { .. } | Self::MongoCdc { .. } => DbType::Mongo,
        }
    }
}
