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
}
