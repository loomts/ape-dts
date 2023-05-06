use crate::meta::db_enums::DbType;

#[derive(Clone)]
pub enum SinkerConfig {
    BasicConfig {
        url: String,
        db_type: DbType,
    },

    Mysql {
        url: String,
        batch_size: usize,
    },

    Pg {
        url: String,
        batch_size: usize,
    },

    Mongo {
        url: String,
        batch_size: usize,
    },

    MysqlCheck {
        url: String,
        batch_size: usize,
        check_log_dir: Option<String>,
    },

    PgCheck {
        url: String,
        batch_size: usize,
        check_log_dir: Option<String>,
    },

    Kafka {
        url: String,
        batch_size: usize,
        ack_timeout_secs: u64,
        required_acks: String,
    },

    OpenFaas {
        url: String,
        batch_size: usize,
        timeout_secs: u64,
    },

    Foxlake {
        batch_size: usize,
        bucket: String,
        access_key: String,
        secret_key: String,
        region: String,
        root_dir: String,
    },
}
