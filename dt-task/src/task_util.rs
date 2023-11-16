use std::{str::FromStr, time::Duration};

use dt_common::{
    config::{config_enums::DbType, sinker_config::SinkerConfig, task_config::TaskConfig},
    error::Error,
};
use dt_connector::sinker::redis::cmd_encoder::CmdEncoder;
use dt_meta::{
    mongo::mongo_constant::MongoConstants, mysql::mysql_meta_manager::MysqlMetaManager,
    pg::pg_meta_manager::PgMetaManager, rdb_meta_manager::RdbMetaManager,
    redis::redis_object::RedisCmd,
};
use mongodb::options::ClientOptions;
use redis::ConnectionLike;
use regex::Regex;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, MySql, Pool, Postgres,
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

    pub async fn create_redis_conn(url: &str) -> Result<redis::Connection, Error> {
        let conn = redis::Client::open(url).unwrap().get_connection().unwrap();
        Ok(conn)
    }

    pub fn get_redis_version(conn: &mut redis::Connection) -> Result<f32, Error> {
        let cmd = RedisCmd::from_str_args(&vec!["INFO"]);
        let value = conn.req_packed_command(&CmdEncoder::encode(&cmd)).unwrap();
        if let redis::Value::Data(data) = value {
            let info = String::from_utf8(data).unwrap();
            let re = Regex::new(r"redis_version:(\S+)").unwrap();
            let cap = re.captures(&info).unwrap();

            let version_str = cap[1].to_string();
            let tokens: Vec<&str> = version_str.split(".").collect();
            if tokens.is_empty() {
                return Err(Error::Unexpected(
                    "can not get redis version by INFO".into(),
                ));
            }

            let mut version = tokens[0].to_string();
            if tokens.len() > 1 {
                version = format!("{}.{}", tokens[0], tokens[1]);
            }
            return Ok(f32::from_str(&version).unwrap());
        }
        Err(Error::Unexpected(
            "can not get redis version by INFO".into(),
        ))
    }

    pub async fn create_rdb_meta_manager(config: &TaskConfig) -> Result<RdbMetaManager, Error> {
        let log_level = &config.runtime.log_level;
        let meta_manager = match &config.sinker {
            SinkerConfig::Mysql { url, .. } | SinkerConfig::MysqlCheck { url, .. } => {
                let mysql_meta_manager =
                    Self::create_mysql_meta_manager(url, log_level, DbType::Mysql).await?;
                RdbMetaManager::from_mysql(mysql_meta_manager)
            }

            SinkerConfig::Starrocks { url, .. } => {
                let mysql_meta_manager =
                    Self::create_mysql_meta_manager(url, log_level, DbType::StarRocks).await?;
                RdbMetaManager::from_mysql(mysql_meta_manager)
            }

            SinkerConfig::Pg { url, .. } | SinkerConfig::PgCheck { url, .. } => {
                let pg_meta_manager = Self::create_pg_meta_manager(url, log_level).await?;
                RdbMetaManager::from_pg(pg_meta_manager)
            }

            _ => {
                return Err(Error::ConfigError("unsupported sinker config".into()));
            }
        };
        Ok(meta_manager)
    }

    pub async fn create_mysql_meta_manager(
        url: &str,
        log_level: &str,
        db_type: DbType,
    ) -> Result<MysqlMetaManager, Error> {
        let enable_sqlx_log = Self::check_enable_sqlx_log(log_level);
        let conn_pool = Self::create_mysql_conn_pool(url, 1, enable_sqlx_log).await?;
        MysqlMetaManager::new_mysql_compatible(conn_pool.clone(), db_type)
            .init()
            .await
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
        // app_name only for debug usage
        client_options.app_name = Some(MongoConstants::APP_NAME.to_string());
        client_options.direct_connection = Some(true);
        Ok(mongodb::Client::with_options(client_options).unwrap())
    }

    #[inline(always)]
    pub fn check_enable_sqlx_log(log_level: &str) -> bool {
        log_level == "debug" || log_level == "trace"
    }
}
