use std::sync::{atomic::AtomicBool, Arc, Mutex};

use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::config_enums::DbType, error::Error, syncer::Syncer, utils::rdb_filter::RdbFilter,
};
use dt_connector::extractor::{
    mongo::{
        mongo_cdc_extractor::MongoCdcExtractor, mongo_snapshot_extractor::MongoSnapshotExtractor,
    },
    mysql::{
        mysql_cdc_extractor::MysqlCdcExtractor, mysql_check_extractor::MysqlCheckExtractor,
        mysql_snapshot_extractor::MysqlSnapshotExtractor,
        mysql_struct_extractor::MysqlStructExtractor,
    },
    pg::{
        pg_cdc_extractor::PgCdcExtractor, pg_check_extractor::PgCheckExtractor,
        pg_snapshot_extractor::PgSnapshotExtractor, pg_struct_extractor::PgStructExtractor,
    },
    redis::{
        redis_cdc_extractor::RedisCdcExtractor, redis_snapshot_extractor::RedisSnapshotExtractor,
    },
    snapshot_resumer::SnapshotResumer,
};
use dt_meta::{
    dt_data::DtData, mysql::mysql_meta_manager::MysqlMetaManager,
    pg::pg_meta_manager::PgMetaManager,
};
use futures::TryStreamExt;
use sqlx::Row;

use super::task_util::TaskUtil;

pub struct ExtractorUtil {}

const MYSQL_SYS_DBS: [&str; 4] = ["information_schema", "mysql", "performance_schema", "sys"];
const PG_SYS_SCHEMAS: [&str; 2] = ["pg_catalog", "information_schema"];

impl ExtractorUtil {
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
        Ok(tbs)
    }

    async fn list_mongo_dbs(url: &str) -> Result<Vec<String>, Error> {
        let client = TaskUtil::create_mongo_client(url).await.unwrap();
        let dbs = client.list_database_names(None, None).await.unwrap();
        Ok(dbs)
    }

    async fn list_mongo_tbs(url: &str, db: &str) -> Result<Vec<String>, Error> {
        let client = TaskUtil::create_mongo_client(url).await.unwrap();
        let tbs = client
            .database(db)
            .list_collection_names(None)
            .await
            .unwrap();
        Ok(tbs)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_mysql_cdc_extractor(
        url: &str,
        binlog_filename: &str,
        binlog_position: u32,
        server_id: u64,
        buffer: Arc<ConcurrentQueue<DtData>>,
        filter: RdbFilter,
        log_level: &str,
        shut_down: Arc<AtomicBool>,
    ) -> Result<MysqlCdcExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = MysqlMetaManager::new(conn_pool).init().await?;

        Ok(MysqlCdcExtractor {
            meta_manager,
            buffer,
            filter,
            url: url.to_string(),
            binlog_filename: binlog_filename.to_string(),
            binlog_position,
            server_id,
            shut_down,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_pg_cdc_extractor(
        url: &str,
        slot_name: &str,
        start_lsn: &str,
        heartbeat_interval_secs: u64,
        buffer: Arc<ConcurrentQueue<DtData>>,
        filter: RdbFilter,
        log_level: &str,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
    ) -> Result<PgCdcExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        Ok(PgCdcExtractor {
            meta_manager,
            buffer,
            filter,
            url: url.to_string(),
            slot_name: slot_name.to_string(),
            start_lsn: start_lsn.to_string(),
            shut_down,
            syncer,
            heartbeat_interval_secs,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_mysql_snapshot_extractor(
        url: &str,
        db: &str,
        tb: &str,
        slice_size: usize,
        resumer: SnapshotResumer,
        buffer: Arc<ConcurrentQueue<DtData>>,
        log_level: &str,
        shut_down: Arc<AtomicBool>,
    ) -> Result<MysqlSnapshotExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        // max_connections: 1 for extracting data from table, 1 for db-meta-manager
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;

        Ok(MysqlSnapshotExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            resumer,
            buffer,
            db: db.to_string(),
            tb: tb.to_string(),
            slice_size,
            shut_down,
        })
    }

    pub async fn create_mysql_check_extractor(
        url: &str,
        check_log_dir: &str,
        batch_size: usize,
        buffer: Arc<ConcurrentQueue<DtData>>,
        log_level: &str,
        shut_down: Arc<AtomicBool>,
    ) -> Result<MysqlCheckExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;

        Ok(MysqlCheckExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            buffer,
            check_log_dir: check_log_dir.to_string(),
            batch_size,
            shut_down,
        })
    }

    pub async fn create_pg_check_extractor(
        url: &str,
        check_log_dir: &str,
        batch_size: usize,
        buffer: Arc<ConcurrentQueue<DtData>>,
        log_level: &str,
        shut_down: Arc<AtomicBool>,
    ) -> Result<PgCheckExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        Ok(PgCheckExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            check_log_dir: check_log_dir.to_string(),
            buffer,
            batch_size,
            shut_down,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_pg_snapshot_extractor(
        url: &str,
        db: &str,
        tb: &str,
        slice_size: usize,
        resumer: SnapshotResumer,
        buffer: Arc<ConcurrentQueue<DtData>>,
        log_level: &str,
        shut_down: Arc<AtomicBool>,
    ) -> Result<PgSnapshotExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        Ok(PgSnapshotExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            resumer,
            buffer,
            slice_size,
            schema: db.to_string(),
            tb: tb.to_string(),
            shut_down,
        })
    }

    pub async fn create_mongo_snapshot_extractor(
        url: &str,
        db: &str,
        tb: &str,
        resumer: SnapshotResumer,
        buffer: Arc<ConcurrentQueue<DtData>>,
        shut_down: Arc<AtomicBool>,
    ) -> Result<MongoSnapshotExtractor, Error> {
        let mongo_client = TaskUtil::create_mongo_client(url).await.unwrap();
        Ok(MongoSnapshotExtractor {
            buffer,
            resumer,
            db: db.to_string(),
            tb: tb.to_string(),
            shut_down,
            mongo_client,
        })
    }

    pub async fn create_mongo_cdc_extractor(
        url: &str,
        resume_token: &str,
        start_timestamp: &i64,
        buffer: Arc<ConcurrentQueue<DtData>>,
        filter: RdbFilter,
        shut_down: Arc<AtomicBool>,
    ) -> Result<MongoCdcExtractor, Error> {
        let mongo_client = TaskUtil::create_mongo_client(url).await.unwrap();
        Ok(MongoCdcExtractor {
            buffer,
            filter,
            resume_token: resume_token.to_string(),
            start_timestamp: *start_timestamp,
            shut_down,
            mongo_client,
        })
    }

    pub async fn create_mysql_struct_extractor(
        url: &str,
        db: &str,
        buffer: Arc<ConcurrentQueue<DtData>>,
        filter: RdbFilter,
        log_level: &str,
        shut_down: Arc<AtomicBool>,
    ) -> Result<MysqlStructExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        // TODO, pass max_connections as parameter
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 2, enable_sqlx_log).await?;

        Ok(MysqlStructExtractor {
            conn_pool,
            buffer,
            db: db.to_string(),
            filter,
            shut_down,
        })
    }

    pub async fn create_pg_struct_extractor(
        url: &str,
        db: &str,
        buffer: Arc<ConcurrentQueue<DtData>>,
        filter: RdbFilter,
        log_level: &str,
        shut_down: Arc<AtomicBool>,
    ) -> Result<PgStructExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        // TODO, pass max_connections as parameter
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;

        Ok(PgStructExtractor {
            conn_pool,
            buffer,
            db: db.to_string(),
            filter,
            shut_down,
        })
    }

    pub async fn create_redis_snapshot_extractor(
        url: &str,
        buffer: Arc<ConcurrentQueue<DtData>>,
        shut_down: Arc<AtomicBool>,
    ) -> Result<RedisSnapshotExtractor, Error> {
        let conn = TaskUtil::create_redis_conn(url).await?;
        Ok(RedisSnapshotExtractor {
            conn,
            buffer,
            shut_down,
        })
    }

    pub async fn create_redis_cdc_extractor(
        url: &str,
        run_id: &str,
        repl_offset: u64,
        heartbeat_interval_secs: u64,
        buffer: Arc<ConcurrentQueue<DtData>>,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
    ) -> Result<RedisCdcExtractor, Error> {
        let conn = TaskUtil::create_redis_conn(url).await?;
        Ok(RedisCdcExtractor {
            conn,
            buffer,
            run_id: run_id.to_string(),
            repl_offset,
            heartbeat_interval_secs,
            shut_down,
            syncer,
        })
    }
}
