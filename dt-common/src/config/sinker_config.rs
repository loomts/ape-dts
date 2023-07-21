use super::config_enums::{ConflictPolicyEnum, DbType};

#[derive(Clone)]
pub enum SinkerConfig {
    Basic {
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

    Redis {
        url: String,
        batch_size: usize,
    },
}

impl SinkerConfig {
    pub fn get_db_type(&self) -> DbType {
        match self {
            Self::Mysql { .. } | Self::MysqlCheck { .. } => DbType::Mysql,
            Self::Pg { .. } | Self::PgCheck { .. } => DbType::Pg,
            Self::Mongo { .. } => DbType::Mongo,
            Self::Kafka { .. } => DbType::Kafka,
            Self::OpenFaas { .. } => DbType::OpenFaas,
            Self::Foxlake { .. } => DbType::Foxlake,
            Self::MysqlStruct { .. } => DbType::Mysql,
            Self::PgStruct { .. } => DbType::Pg,
            Self::Redis { .. } => DbType::Redis,
            Self::Basic { db_type, .. } => db_type.clone(),
        }
    }
}
