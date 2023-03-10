use std::{
    str::FromStr,
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::Duration,
};

use concurrent_queue::ConcurrentQueue;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, MySql, Pool, Postgres,
};

use crate::{
    config::runtime_config::RuntimeConfig,
    error::Error,
    extractor::{
        mysql::{
            mysql_cdc_extractor::MysqlCdcExtractor,
            mysql_snapshot_extractor::MysqlSnapshotExtractor,
        },
        pg::{pg_cdc_extractor::PgCdcExtractor, pg_snapshot_extractor::PgSnapshotExtractor},
        rdb_filter::RdbFilter,
    },
    meta::{
        mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager,
        row_data::RowData,
    },
    metric::Metric,
    sinker::{
        mysql_sinker::MysqlSinker, parallel_sinker::ParallelSinker, pg_sinker::PgSinker,
        rdb_partitioner::RdbPartitioner, rdb_router::RdbRouter,
    },
    traits::Sinker,
};

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

    pub async fn create_mysql_cdc_extractor<'a>(
        url: &str,
        binlog_filename: &str,
        binlog_position: u32,
        server_id: u64,
        buffer: &'a ConcurrentQueue<RowData>,
        filter: RdbFilter,
        log_level: &str,
        shut_down: &'a AtomicBool,
    ) -> Result<MysqlCdcExtractor<'a>, Error> {
        let enable_sqlx_log = Self::check_enable_sqlx_log(log_level);
        let conn_pool = Self::create_mysql_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = MysqlMetaManager::new(conn_pool).init().await?;

        Ok(MysqlCdcExtractor {
            meta_manager,
            buffer,
            filter,
            url: url.to_string(),
            binlog_filename: binlog_filename.to_string(),
            binlog_position,
            server_id,
            shut_down: &shut_down,
        })
    }

    pub async fn create_pg_cdc_extractor<'a>(
        url: &str,
        slot_name: &str,
        start_sln: &str,
        buffer: &'a ConcurrentQueue<RowData>,
        filter: RdbFilter,
        log_level: &str,
        shut_down: &'a AtomicBool,
        metric: Arc<Mutex<Metric>>,
    ) -> Result<PgCdcExtractor<'a>, Error> {
        let enable_sqlx_log = Self::check_enable_sqlx_log(log_level);
        let conn_pool = Self::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        Ok(PgCdcExtractor {
            meta_manager,
            buffer,
            filter,
            url: url.to_string(),
            slot_name: slot_name.to_string(),
            start_sln: start_sln.to_string(),
            shut_down: &shut_down,
            metric,
        })
    }

    pub async fn create_mysql_snapshot_extractor<'a>(
        url: &str,
        do_tb: &str,
        slice_size: usize,
        buffer: &'a ConcurrentQueue<RowData>,
        log_level: &str,
        shut_down: &'a AtomicBool,
    ) -> Result<MysqlSnapshotExtractor<'a>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        // max_connections: 1 for extracting data from table, 1 for db-meta-manager
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;

        let (db, tb) = Self::parse_do_tb(do_tb)?;
        Ok(MysqlSnapshotExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            buffer,
            db,
            tb,
            slice_size,
            shut_down: &&shut_down,
        })
    }

    pub async fn create_pg_snapshot_extractor<'a>(
        url: &str,
        do_tb: &str,
        slice_size: usize,
        buffer: &'a ConcurrentQueue<RowData>,
        log_level: &str,
        shut_down: &'a AtomicBool,
    ) -> Result<PgSnapshotExtractor<'a>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        let (db, tb) = Self::parse_do_tb(do_tb)?;
        Ok(PgSnapshotExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            buffer,
            slice_size,
            schema: db,
            tb,
            shut_down: &&shut_down,
        })
    }

    pub async fn create_mysql_sinker<'a>(
        url: &str,
        buffer: &'a ConcurrentQueue<RowData>,
        router: &RdbRouter,
        runtime_config: &RuntimeConfig,
        shut_down: &'a AtomicBool,
        metric: Arc<Mutex<Metric>>,
    ) -> Result<ParallelSinker<'a>, Error> {
        let enable_sqlx_log = Self::check_enable_sqlx_log(&runtime_config.log_level);
        let conn_pool = Self::create_mysql_conn_pool(
            url,
            runtime_config.parallel_size as u32 * 2,
            enable_sqlx_log,
        )
        .await?;

        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;
        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..runtime_config.parallel_size {
            let sinker = MysqlSinker {
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                router: router.clone(),
                batch_size: runtime_config.batch_size,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }

        let partitioner = RdbPartitioner::new_for_mysql(meta_manager);
        Ok(ParallelSinker {
            buffer,
            partitioner: Box::new(partitioner),
            sub_sinkers,
            shut_down,
            metric,
        })
    }

    pub async fn create_pg_sinker<'a>(
        url: &str,
        buffer: &'a ConcurrentQueue<RowData>,
        router: &RdbRouter,
        runtime_config: &RuntimeConfig,
        shut_down: &'a AtomicBool,
        metric: Arc<Mutex<Metric>>,
    ) -> Result<ParallelSinker<'a>, Error> {
        let enable_sqlx_log = Self::check_enable_sqlx_log(&runtime_config.log_level);
        let conn_pool = Self::create_pg_conn_pool(
            url,
            runtime_config.parallel_size as u32 * 2,
            enable_sqlx_log,
        )
        .await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..runtime_config.parallel_size {
            let sinker = PgSinker {
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                router: router.clone(),
                batch_size: runtime_config.batch_size,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }

        let partitioner = RdbPartitioner::new_for_pg(meta_manager.clone());
        Ok(ParallelSinker {
            buffer,
            partitioner: Box::new(partitioner),
            sub_sinkers,
            shut_down,
            metric,
        })
    }

    #[inline(always)]
    pub async fn sleep_millis(millis: u64) {
        tokio::time::sleep(Duration::from_millis(millis)).await;
    }

    #[inline(always)]
    fn check_enable_sqlx_log(log_level: &str) -> bool {
        log_level == "debug" || log_level == "trace"
    }

    fn parse_do_tb(do_tb: &str) -> Result<(String, String), Error> {
        let vec = do_tb.split(".").collect::<Vec<&str>>();
        let db = vec.get(0).unwrap().to_string();
        let tb = vec.get(1).unwrap().to_string();
        Ok((db, tb))
    }
}
