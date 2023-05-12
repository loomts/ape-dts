use crate::meta::db_enums::DbType;

use super::config_enums::ConflictPolicyEnum;

#[derive(Clone)]
pub enum SinkerConfig {
    // TODO, split this config into mysql/pg/...
    BasicConfig {
        url: String,
        db_type: DbType,
        conflict_policy: ConflictPolicyEnum,
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
