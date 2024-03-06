use std::{str::FromStr, time::Duration};

use dt_common::{
    config::{config_enums::DbType, sinker_config::SinkerConfig, task_config::TaskConfig},
    error::Error,
};
use dt_meta::{
    mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager,
    rdb_meta_manager::RdbMetaManager,
};
use futures::TryStreamExt;
use mongodb::options::ClientOptions;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, MySql, Pool, Postgres, Row,
};

use crate::redis_util::RedisUtil;

const MYSQL_SYS_DBS: [&str; 4] = ["information_schema", "mysql", "performance_schema", "sys"];
const PG_SYS_SCHEMAS: [&str; 2] = ["pg_catalog", "information_schema"];

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
        RedisUtil::create_redis_conn(url).await
    }

    pub fn get_redis_version(conn: &mut redis::Connection) -> Result<f32, Error> {
        RedisUtil::get_redis_version(conn)
    }

    pub async fn create_rdb_meta_manager(
        config: &TaskConfig,
    ) -> Result<Option<RdbMetaManager>, Error> {
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
                return Ok(None);
            }
        };
        Ok(Some(meta_manager))
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

    pub async fn create_mongo_client(url: &str, app_name: &str) -> Result<mongodb::Client, Error> {
        let mut client_options = ClientOptions::parse_async(url).await.unwrap();
        // app_name only for debug usage
        client_options.app_name = Some(app_name.to_string());
        client_options.direct_connection = Some(true);
        Ok(mongodb::Client::with_options(client_options).unwrap())
    }

    pub fn check_enable_sqlx_log(log_level: &str) -> bool {
        log_level == "debug" || log_level == "trace"
    }

    pub async fn list_dbs(url: &str, db_type: &DbType) -> Result<Vec<String>, Error> {
        let mut dbs = match db_type {
            DbType::Mysql => Self::list_mysql_dbs(url).await?,
            DbType::Pg => Self::list_pg_schemas(url).await?,
            DbType::Mongo => Self::list_mongo_dbs(url).await?,
            _ => Vec::new(),
        };
        dbs.sort();
        Ok(dbs)
    }

    pub async fn list_tbs(url: &str, db: &str, db_type: &DbType) -> Result<Vec<String>, Error> {
        let mut tbs = match db_type {
            DbType::Mysql => Self::list_mysql_tbs(url, db).await?,
            DbType::Pg => Self::list_pg_tbs(url, db).await?,
            DbType::Mongo => Self::list_mongo_tbs(url, db).await?,
            _ => Vec::new(),
        };
        tbs.sort();
        Ok(tbs)
    }

    async fn list_pg_schemas(url: &str) -> Result<Vec<String>, Error> {
        let mut schemas = Vec::new();
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 1, false).await?;

        let sql = "SELECT schema_name
            FROM information_schema.schemata
            WHERE catalog_name = current_database()";
        let mut rows = sqlx::query(sql).fetch(&conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let schema: String = row.try_get(0)?;
            if PG_SYS_SCHEMAS.contains(&schema.as_str()) {
                continue;
            }
            schemas.push(schema);
        }
        conn_pool.close().await;
        Ok(schemas)
    }

    async fn list_pg_tbs(url: &str, schema: &str) -> Result<Vec<String>, Error> {
        let mut tbs = Vec::new();
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 1, false).await?;

        let sql = format!(
            "SELECT table_name 
            FROM information_schema.tables
            WHERE table_catalog = current_database() 
            AND table_schema = '{}'",
            schema
        );
        let mut rows = sqlx::query(&sql).fetch(&conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let tb: String = row.try_get(0)?;
            tbs.push(tb);
        }
        conn_pool.close().await;
        Ok(tbs)
    }

    async fn list_mysql_dbs(url: &str) -> Result<Vec<String>, Error> {
        let mut dbs = Vec::new();
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 1, false).await?;

        let sql = "SHOW DATABASES";
        let mut rows = sqlx::query(sql).fetch(&conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let db: String = row.try_get(0)?;
            if MYSQL_SYS_DBS.contains(&db.as_str()) {
                continue;
            }
            dbs.push(db);
        }
        conn_pool.close().await;
        Ok(dbs)
    }

    async fn list_mysql_tbs(url: &str, db: &str) -> Result<Vec<String>, Error> {
        let mut tbs = Vec::new();
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 1, false).await?;

        let sql = format!("SHOW TABLES IN `{}`", db);
        let mut rows = sqlx::query(&sql).fetch(&conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let tb: String = row.try_get(0)?;
            tbs.push(tb);
        }
        conn_pool.close().await;
        Ok(tbs)
    }

    async fn list_mongo_dbs(url: &str) -> Result<Vec<String>, Error> {
        let client = TaskUtil::create_mongo_client(url, "").await.unwrap();
        let dbs = client.list_database_names(None, None).await.unwrap();
        Ok(dbs)
    }

    async fn list_mongo_tbs(url: &str, db: &str) -> Result<Vec<String>, Error> {
        let client = TaskUtil::create_mongo_client(url, "").await.unwrap();
        let tbs = client
            .database(db)
            .list_collection_names(None)
            .await
            .unwrap();
        Ok(tbs)
    }
}
