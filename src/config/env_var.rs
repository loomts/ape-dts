use std::env;

use crate::error::Error;

pub struct EnvVar {
    pub sqlx_log: String,
    pub log4rs_file: String,
    pub task_config: String,
    pub task_type: String,
}

impl EnvVar {
    pub const SQLX_LOG: &str = "SQLX_LOG";
    pub const LOG4RS_FILE: &str = "LOG4RS_FILE";
    pub const TASK_CONFIG: &str = "TASK_CONFIG";
    pub const TASK_TYPE: &str = "TASK_TYPE";

    pub fn new() -> Result<Self, Error> {
        let sqlx_log = env::var(EnvVar::SQLX_LOG)?;
        let log4rs_file = env::var(EnvVar::LOG4RS_FILE).unwrap();
        let task_config = env::var(EnvVar::TASK_CONFIG).unwrap();
        let task_type = env::var(EnvVar::TASK_TYPE).unwrap();

        Ok(Self {
            sqlx_log,
            log4rs_file,
            task_config,
            task_type,
        })
    }

    pub fn is_sqlx_log_enabled(&self) -> bool {
        "enable" == self.sqlx_log.to_lowercase()
    }
}
