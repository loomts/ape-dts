use std::{str::FromStr, time::Duration};

use dt_common::config::{sinker_config::SinkerConfig, task_config::TaskConfig};
use mongodb::options::ClientOptions;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, MySql, Pool, Postgres,
};

use crate::{
    common::constants::Mongo,
    error::Error,
    meta::{
        mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager,
        rdb_meta_manager::RdbMetaManager,
    },
};

pub struct TaskUtil {}

impl TaskUtil {
    pub async fn create_mysql_conn_pool(
        url: &str,
        max_connections: u32,
        enable_sqlx_log: bool,
    ) -> Result<Pool<MySql>, Error> {
        let mut conn_options = MySqlConnectOptions::from_str(url)?;
        // The default character set is `utf8mb4`
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

    pub async fn create_pg_conn_pool(
        url: &str,
        max_connections: u32,
        enable_sqlx_log: bool,
    ) -> Result<Pool<Postgres>, Error> {
        let mut conn_options = PgConnectOptions::from_str(url)?;
        conn_options
            .log_statements(log::LevelFilter::Info)
            .log_slow_statements(log::LevelFilter::Info, Duration::from_secs(1));

        if !enable_sqlx_log {
            conn_options.disable_statement_logging();
        }

        let conn_pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(conn_options)
            .await?;
        Ok(conn_pool)
    }

    pub async fn create_rdb_meta_manager(config: &TaskConfig) -> Result<RdbMetaManager, Error> {
        let log_level = &config.runtime.log_level;
        let meta_manager = match &config.sinker {
            SinkerConfig::Mysql { url, .. } | SinkerConfig::MysqlCheck { url, .. } => {
                let mysql_meta_manager = Self::create_mysql_meta_manager(&url, &log_level).await?;
                RdbMetaManager::from_mysql(mysql_meta_manager)
            }

            SinkerConfig::Pg { url, .. } | SinkerConfig::PgCheck { url, .. } => {
                let pg_meta_manager = Self::create_pg_meta_manager(&url, &log_level).await?;
                RdbMetaManager::from_pg(pg_meta_manager)
            }

            _ => {
                return Err(Error::Unexpected {
                    error: "unexpected sinker type".to_string(),
                });
            }
        };
        Ok(meta_manager)
    }

    pub async fn create_mysql_meta_manager(
        url: &str,
        log_level: &str,
    ) -> Result<MysqlMetaManager, Error> {
        let enable_sqlx_log = Self::check_enable_sqlx_log(log_level);
        let conn_pool = Self::create_mysql_conn_pool(url, 1, enable_sqlx_log).await?;
        MysqlMetaManager::new(conn_pool.clone()).init().await
    }

    pub async fn create_pg_meta_manager(
        url: &str,
        log_level: &str,
    ) -> Result<PgMetaManager, Error> {
        let enable_sqlx_log = Self::check_enable_sqlx_log(log_level);
        let conn_pool = Self::create_pg_conn_pool(url, 1, enable_sqlx_log).await?;
        PgMetaManager::new(conn_pool.clone()).init().await
    }

    pub async fn create_mongo_client(url: &str) -> Result<mongodb::Client, Error> {
        let mut client_options = ClientOptions::parse_async(url).await.unwrap();
        client_options.app_name = Some(Mongo::APP_NAME.to_string());
        Ok(mongodb::Client::with_options(client_options).unwrap())
    }

    #[inline(always)]
    pub async fn sleep_millis(millis: u64) {
        tokio::time::sleep(Duration::from_millis(millis)).await;
    }

    #[inline(always)]
    pub fn check_enable_sqlx_log(log_level: &str) -> bool {
        log_level == "debug" || log_level == "trace"
    }
}
