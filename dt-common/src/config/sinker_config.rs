use super::config_enums::{ConflictPolicyEnum, DbType};

#[derive(Clone, Debug)]
pub enum SinkerConfig {
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
        app_name: String,
        batch_size: usize,
    },

    MysqlCheck {
        url: String,
        batch_size: usize,
        check_log_dir: String,
    },

    PgCheck {
        url: String,
        batch_size: usize,
        check_log_dir: String,
    },

    MongoCheck {
        url: String,
        app_name: String,
        batch_size: usize,
        check_log_dir: String,
    },

    MysqlStruct {
        url: String,
        conflict_policy: ConflictPolicyEnum,
    },

    PgStruct {
        url: String,
        conflict_policy: ConflictPolicyEnum,
    },

    Kafka {
        url: String,
        batch_size: usize,
        ack_timeout_secs: u64,
        required_acks: String,
    },

    Redis {
        url: String,
        batch_size: usize,
        method: String,
        is_cluster: bool,
    },

    RedisStatistic {
        data_size_threshold: usize,
        statistic_log_dir: String,
    },

    Starrocks {
        url: String,
        batch_size: usize,
        stream_load_url: String,
    },
}

#[derive(Clone, Debug)]
pub struct BasicSinkerConfig {
    pub db_type: DbType,
    pub url: String,
    pub batch_size: usize,
}
