use std::sync::{atomic::AtomicBool, Arc, Mutex};

use concurrent_queue::ConcurrentQueue;

use crate::{
    error::Error,
    extractor::{
        mysql::{
            mysql_cdc_extractor::MysqlCdcExtractor, mysql_check_extractor::MysqlCheckExtractor,
            mysql_snapshot_extractor::MysqlSnapshotExtractor,
        },
        pg::{
            pg_cdc_extractor::PgCdcExtractor, pg_check_extractor::PgCheckExtractor,
            pg_snapshot_extractor::PgSnapshotExtractor,
        },
        rdb_filter::RdbFilter,
    },
    meta::{
        mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager,
        row_data::RowData,
    },
    metric::Metric,
};

use super::task_util::TaskUtil;

pub struct ExtractorUtil {}

impl ExtractorUtil {
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
            shut_down: &shut_down,
        })
    }

    pub async fn create_pg_cdc_extractor<'a>(
        url: &str,
        slot_name: &str,
        start_sln: &str,
        heartbeat_interval_secs: u64,
        buffer: &'a ConcurrentQueue<RowData>,
        filter: RdbFilter,
        log_level: &str,
        shut_down: &'a AtomicBool,
        metric: Arc<Mutex<Metric>>,
    ) -> Result<PgCdcExtractor<'a>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;
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
            heartbeat_interval_secs,
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

    pub async fn create_mysql_check_extractor<'a>(
        url: &str,
        check_log_dir: &str,
        slice_size: usize,
        buffer: &'a ConcurrentQueue<RowData>,
        log_level: &str,
        shut_down: &'a AtomicBool,
    ) -> Result<MysqlCheckExtractor<'a>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;

        Ok(MysqlCheckExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            buffer,
            check_log_dir: check_log_dir.to_string(),
            slice_size,
            shut_down: &&shut_down,
        })
    }

    pub async fn create_pg_check_extractor<'a>(
        url: &str,
        check_log_dir: &str,
        slice_size: usize,
        buffer: &'a ConcurrentQueue<RowData>,
        log_level: &str,
        shut_down: &'a AtomicBool,
    ) -> Result<PgCheckExtractor<'a>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        Ok(PgCheckExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            check_log_dir: check_log_dir.to_string(),
            buffer,
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

    #[inline(always)]
    fn parse_do_tb(do_tb: &str) -> Result<(String, String), Error> {
        let vec = do_tb.split(".").collect::<Vec<&str>>();
        let db = vec.get(0).unwrap().to_string();
        let tb = vec.get(1).unwrap().to_string();
        Ok((db, tb))
    }
}
