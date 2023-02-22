use std::{str::FromStr, time::Duration};

use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    ConnectOptions, MySql, Pool,
};

use crate::error::Error;

pub struct TaskUtil {}

impl TaskUtil {
    pub async fn create_mysql_conn_pool(
        url: &str,
        max_connections: u32,
        enable_sqlx_log: bool,
    ) -> Result<Pool<MySql>, Error> {
        let mut conn_options = MySqlConnectOptions::from_str(url)?;
        conn_options
            .log_statements(log::LevelFilter::Info)
            .log_slow_statements(log::LevelFilter::Info, Duration::from_secs(1));

        if !enable_sqlx_log {
            conn_options.disable_statement_logging();
        }

        let conn_pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(conn_options)
            .await?;
        Ok(conn_pool)
    }

    pub async fn sleep_millis(millis: u64) {
        async_std::task::sleep(Duration::from_millis(millis)).await;
        // tokio::time::sleep(Duration::from_millis(1)).await;
    }

    pub fn check_enable_sqlx_log(log_level: &str) -> bool {
        log_level == "debug" || log_level == "trace"
    }
}
