use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use chrono::{Duration, Utc};
use dt_common::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser,
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    meta::{ddl_meta::ddl_type::DdlType, time::dt_utc_time::DtNaiveTime},
    utils::{sql_util::SqlUtil, time_util::TimeUtil},
};

use dt_common::meta::{
    col_value::ColValue, ddl_meta::ddl_parser::DdlParser,
    mysql::mysql_meta_manager::MysqlMetaManager, row_data::RowData,
};
use dt_connector::rdb_router::RdbRouter;
use dt_task::task_util::TaskUtil;

use sqlx::{MySql, Pool, Postgres};
use tokio::task::JoinHandle;

use crate::test_config_util::TestConfigUtil;

use super::{base_test_runner::BaseTestRunner, rdb_util::RdbUtil};

pub struct RdbTestRunner {
    pub base: BaseTestRunner,
    pub src_conn_pool_mysql: Option<Pool<MySql>>,
    pub dst_conn_pool_mysql: Option<Pool<MySql>>,
    pub src_conn_pool_pg: Option<Pool<Postgres>>,
    pub dst_conn_pool_pg: Option<Pool<Postgres>>,
    pub router: RdbRouter,
}

pub const SRC: &str = "src";
pub const DST: &str = "dst";
pub const PUBLIC: &str = "public";

const UTC_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[allow(dead_code)]
impl RdbTestRunner {
    pub async fn new_default(relative_test_dir: &str) -> anyhow::Result<Self> {
        Self::new(relative_test_dir, true).await
    }

    pub async fn new(relative_test_dir: &str, recreate_pub_and_slot: bool) -> anyhow::Result<Self> {
        let mut base = BaseTestRunner::new(relative_test_dir).await.unwrap();

        // prepare conn pools
        let mut src_conn_pool_mysql = None;
        let mut dst_conn_pool_mysql = None;
        let mut src_conn_pool_pg = None;
        let mut dst_conn_pool_pg = None;

        let (src_db_type, src_url, dst_db_type, dst_url) = Self::parse_conn_info(&base);
        if src_db_type == DbType::Mysql {
            src_conn_pool_mysql = Some(TaskUtil::create_mysql_conn_pool(&src_url, 1, false).await?);
        } else {
            src_conn_pool_pg = Some(TaskUtil::create_pg_conn_pool(&src_url, 1, false).await?);
        }

        if !dst_url.is_empty() {
            match dst_db_type {
                DbType::Mysql | DbType::Foxlake => {
                    dst_conn_pool_mysql =
                        Some(TaskUtil::create_mysql_conn_pool(&dst_url, 1, false).await?);
                }
                _ => {
                    dst_conn_pool_pg =
                        Some(TaskUtil::create_pg_conn_pool(&dst_url, 1, false).await?);
                }
            }
        }

        let config = TaskConfig::new(&base.task_config_file).unwrap();
        let router = RdbRouter::from_config(&config.router, &dst_db_type).unwrap();

        // for pg cdc, recreate publication & slot before each test
        if let ExtractorConfig::PgCdc {
            slot_name,
            pub_name,
            ..
        } = config.extractor
        {
            if recreate_pub_and_slot {
                let pub_name = if pub_name.is_empty() {
                    format!("{}_publication_for_all_tables", slot_name)
                } else {
                    pub_name.clone()
                };
                let drop_pub_sql = format!("DROP PUBLICATION IF EXISTS {}", pub_name);
                let drop_slot_sql= format!("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{}'", slot_name);
                base.src_prepare_sqls.push(drop_pub_sql);
                base.src_prepare_sqls.push(drop_slot_sql);
            }
        }

        Ok(Self {
            src_conn_pool_mysql,
            dst_conn_pool_mysql,
            src_conn_pool_pg,
            dst_conn_pool_pg,
            router,
            base,
        })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        if let Some(pool) = &self.src_conn_pool_mysql {
            pool.close().await;
        }
        if let Some(pool) = &self.dst_conn_pool_mysql {
            pool.close().await;
        }
        if let Some(pool) = &self.src_conn_pool_pg {
            pool.close().await;
        }
        if let Some(pool) = &self.dst_conn_pool_pg {
            pool.close().await;
        }
        Ok(())
    }

    pub async fn get_dst_mysql_version(&self) -> String {
        if let Some(conn_pool) = &self.dst_conn_pool_mysql {
            let meta_manager = MysqlMetaManager::new(conn_pool.clone())
                .init()
                .await
                .unwrap();
            return meta_manager.version.clone();
        }
        String::new()
    }

    fn parse_conn_info(base: &BaseTestRunner) -> (DbType, String, DbType, String) {
        let mut src_db_type = DbType::Mysql;
        let mut src_url = String::new();
        let mut dst_db_type = DbType::Mysql;
        let mut dst_url = String::new();

        let config = TaskConfig::new(&base.task_config_file).unwrap();
        match config.extractor {
            ExtractorConfig::MysqlCdc { url, .. }
            | ExtractorConfig::MysqlSnapshot { url, .. }
            | ExtractorConfig::MysqlCheck { url, .. }
            | ExtractorConfig::MysqlStruct { url, .. } => {
                src_url = url.clone();
            }

            ExtractorConfig::PgCdc { url, .. }
            | ExtractorConfig::PgSnapshot { url, .. }
            | ExtractorConfig::PgCheck { url, .. }
            | ExtractorConfig::PgStruct { url, .. } => {
                src_db_type = DbType::Pg;
                src_url = url.clone();
            }

            _ => {}
        }

        match config.sinker {
            SinkerConfig::Mysql { url, .. }
            | SinkerConfig::MysqlCheck { url, .. }
            | SinkerConfig::MysqlStruct { url, .. }
            | SinkerConfig::Starrocks { url, .. }
            | SinkerConfig::Foxlake { url, .. }
            | SinkerConfig::FoxlakeStruct { url, .. } => {
                dst_url = url.clone();
            }

            SinkerConfig::Pg { url, .. }
            | SinkerConfig::PgCheck { url, .. }
            | SinkerConfig::PgStruct { url, .. } => {
                dst_db_type = DbType::Pg;
                dst_url = url.clone();
            }

            _ => {}
        }

        (src_db_type, src_url, dst_db_type, dst_url)
    }

    pub async fn run_snapshot_test(&self, compare_data: bool) -> anyhow::Result<()> {
        // prepare src and dst tables
        self.execute_prepare_sqls().await?;
        self.execute_test_sqls().await?;

        // start task
        self.base.start_task().await?;

        // compare data
        let (src_db_tbs, dst_db_tbs) = self.get_compare_db_tbs()?;
        if compare_data {
            assert!(self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?)
        }
        Ok(())
    }

    pub async fn run_ddl_test(&self, start_millis: u64, parse_millis: u64) -> anyhow::Result<()> {
        self.execute_prepare_sqls().await?;

        self.update_cdc_task_config(start_millis, parse_millis);
        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        self.execute_src_sqls(&self.base.src_test_sqls).await?;

        let (mut src_db_tbs, mut dst_db_tbs) = self.get_compare_db_tbs()?;
        let filtered_db_tbs = self.get_filtered_db_tbs();
        for i in (0..src_db_tbs.len()).rev() {
            // do not check filtered tables since they may be dropped
            if filtered_db_tbs.contains(&src_db_tbs[i]) {
                src_db_tbs.remove(i);
                dst_db_tbs.remove(i);
            }
        }

        self.base.wait_task_finish(&task).await?;
        assert!(self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?);
        Ok(())
    }

    pub async fn run_cdc_test(&self, start_millis: u64, parse_millis: u64) -> anyhow::Result<()> {
        // prepare src and dst tables
        self.execute_prepare_sqls().await?;

        // start task
        let task = self.spawn_cdc_task(start_millis, parse_millis).await?;

        self.execute_test_sqls_and_compare(parse_millis).await?;

        self.base.wait_task_finish(&task).await
    }

    pub async fn run_heartbeat_test(
        &self,
        _start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        let config = TaskConfig::new(&self.base.task_config_file).unwrap();
        let (heartbeat_tb, db_type) = match config.extractor {
            ExtractorConfig::PgCdc { heartbeat_tb, .. } => (heartbeat_tb.clone(), DbType::Pg),
            ExtractorConfig::MysqlCdc { heartbeat_tb, .. } => (heartbeat_tb.clone(), DbType::Mysql),
            _ => (String::new(), DbType::Mysql),
        };

        let tokens =
            ConfigTokenParser::parse(&heartbeat_tb, &['.'], &SqlUtil::get_escape_pairs(&db_type));
        let db_tb = (tokens[0].clone(), tokens[1].clone());

        self.execute_prepare_sqls().await?;

        // start task
        self.update_cdc_task_config(0, parse_millis);
        let task = self.base.spawn_task().await?;
        self.base.wait_task_finish(&task).await.unwrap();

        let src_data = self.fetch_data(&db_tb, SRC).await?;
        assert_eq!(src_data.len(), 1);
        Ok(())
    }

    pub fn update_cdc_task_config(&self, start_millis: u64, parse_millis: u64) {
        let duration = Duration::try_milliseconds((start_millis + parse_millis) as i64).unwrap();
        let end_time_utc = (Utc::now() + duration).format(UTC_FORMAT).to_string();
        let update_configs = vec![("extractor", "end_time_utc", end_time_utc.as_str())];
        TestConfigUtil::update_task_config_2(
            &self.base.task_config_file,
            &self.base.task_config_file,
            &update_configs,
        );
    }

    pub async fn spawn_cdc_task(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<JoinHandle<()>> {
        // start task
        let total_parse_millis = self.get_total_parse_millis(parse_millis);
        self.update_cdc_task_config(start_millis, total_parse_millis);
        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;
        Ok(task)
    }

    fn get_total_parse_millis(&self, parse_millis: u64) -> u64 {
        let (src_insert_sqls, src_update_sqls, src_delete_sqls) =
            Self::split_dml_sqls(&self.base.src_test_sqls);
        // parse_millis * 2 for: time to parse binlog + time to compare data
        (!src_insert_sqls.is_empty() as u64
            + !src_update_sqls.is_empty() as u64
            + !src_delete_sqls.is_empty() as u64)
            * parse_millis
            * 2
    }

    pub async fn execute_test_sqls_and_compare(&self, parse_millis: u64) -> anyhow::Result<()> {
        // load dml sqls
        let (src_insert_sqls, src_update_sqls, src_delete_sqls) =
            Self::split_dml_sqls(&self.base.src_test_sqls);

        let (src_db_tbs, dst_db_tbs) = self.get_compare_db_tbs()?;

        // insert src data
        if !src_insert_sqls.is_empty() {
            self.execute_src_sqls(&src_insert_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?;
        }

        // update src data
        if !src_update_sqls.is_empty() {
            self.execute_src_sqls(&src_update_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?;
        }

        // delete src data
        if !src_delete_sqls.is_empty() {
            self.execute_src_sqls(&src_delete_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?;
        }

        Ok(())
    }

    pub fn split_dml_sqls(dml_sqls: &Vec<String>) -> (Vec<String>, Vec<String>, Vec<String>) {
        let mut insert_sqls = Vec::new();
        let mut update_sqls = Vec::new();
        let mut delete_sqls = Vec::new();

        for sql in dml_sqls.iter() {
            if sql.to_lowercase().starts_with("insert") {
                insert_sqls.push(sql.clone());
            } else if sql.to_lowercase().starts_with("update") {
                update_sqls.push(sql.clone());
            } else {
                delete_sqls.push(sql.clone());
            }
        }
        (insert_sqls, update_sqls, delete_sqls)
    }

    pub async fn execute_test_sqls(&self) -> anyhow::Result<()> {
        self.execute_src_sqls(&self.base.src_test_sqls).await?;
        self.execute_dst_sqls(&self.base.dst_test_sqls).await
    }

    pub async fn execute_prepare_sqls(&self) -> anyhow::Result<()> {
        self.execute_src_sqls(&self.base.src_prepare_sqls).await?;
        self.execute_dst_sqls(&self.base.dst_prepare_sqls).await
    }

    pub async fn execute_clean_sqls(&self) -> anyhow::Result<()> {
        self.execute_src_sqls(&self.base.src_clean_sqls).await?;
        self.execute_dst_sqls(&self.base.dst_clean_sqls).await
    }

    pub async fn execute_src_sqls(&self, sqls: &Vec<String>) -> anyhow::Result<()> {
        if let Some(pool) = &self.src_conn_pool_mysql {
            RdbUtil::execute_sqls_mysql(pool, sqls).await?;
        }
        if let Some(pool) = &self.src_conn_pool_pg {
            RdbUtil::execute_sqls_pg(pool, sqls).await?;
        }
        Ok(())
    }

    pub async fn execute_dst_sqls(&self, sqls: &Vec<String>) -> anyhow::Result<()> {
        if let Some(pool) = &self.dst_conn_pool_mysql {
            RdbUtil::execute_sqls_mysql(pool, sqls).await?;
        }
        if let Some(pool) = &self.dst_conn_pool_pg {
            RdbUtil::execute_sqls_pg(pool, sqls).await?;
        }
        Ok(())
    }

    pub async fn compare_data_for_tbs(
        &self,
        src_db_tbs: &Vec<(String, String)>,
        dst_db_tbs: &Vec<(String, String)>,
    ) -> anyhow::Result<bool> {
        let filtered_db_tbs = self.get_filtered_db_tbs();
        for i in 0..src_db_tbs.len() {
            if filtered_db_tbs.contains(&src_db_tbs[i]) {
                let dst_data = self.fetch_data(&dst_db_tbs[i], DST).await?;
                if !dst_data.is_empty() {
                    println!("tb: {:?} is filtered but dst is not emtpy", dst_db_tbs[i]);
                    assert!(false)
                }
            } else {
                assert!(self.compare_tb_data(&src_db_tbs[i], &dst_db_tbs[i]).await?)
            }
        }
        Ok(true)
    }

    async fn compare_tb_data(
        &self,
        src_db_tb: &(String, String),
        dst_db_tb: &(String, String),
    ) -> anyhow::Result<bool> {
        let src_data = self.fetch_data(src_db_tb, SRC).await?;
        let dst_data = self.fetch_data(dst_db_tb, DST).await?;
        println!(
            "comparing row data for src_tb: {:?}, dst_tb: {:?}, src_data count: {}",
            src_db_tb,
            dst_db_tb,
            src_data.len()
        );

        let col_map = self.router.get_col_map(&src_db_tb.0, &src_db_tb.1);
        if !self.compare_row_data(&src_data, &dst_data, col_map) {
            println!(
                "compare tb data failed, src_tb: {:?}, dst_tb: {:?}",
                src_db_tb, dst_db_tb,
            );
            return Ok(false);
        }
        Ok(true)
    }

    fn compare_row_data(
        &self,
        src_data: &Vec<RowData>,
        dst_data: &Vec<RowData>,
        col_map: Option<&HashMap<String, String>>,
    ) -> bool {
        assert_eq!(src_data.len(), dst_data.len());
        let src_db_type = self.get_db_type(SRC);
        let dst_db_type = self.get_db_type(DST);

        for i in 0..src_data.len() {
            let src_col_values = src_data[i].after.as_ref().unwrap();
            let dst_col_values = dst_data[i].after.as_ref().unwrap();

            for (src_col, src_col_value) in src_col_values {
                let dst_col = if let Some(col_map) = col_map {
                    col_map.get(src_col).unwrap_or(src_col)
                } else {
                    src_col
                };

                let dst_col_value = dst_col_values.get(dst_col).unwrap();

                // TODO
                // issue: https://github.com/apecloud/foxlake/issues/2108
                // sqlx will execute: "SET time_zone='+00:00',NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;"
                // to initialize each connection.
                // but it doesn't work on Foxlake
                if matches!(self.base.get_config().sinker, SinkerConfig::Foxlake { .. })
                    && matches!(dst_col_value, ColValue::Timestamp(..))
                {
                    continue;
                }

                if Self::compare_col_value(src_col_value, dst_col_value, &src_db_type, &dst_db_type)
                {
                    continue;
                }
                println!(
                    "row index: {}, col: {}, src_col_value: {:?}, dst_col_value: {:?}",
                    i, src_col, src_col_value, dst_col_value
                );
                return false;
            }
        }
        true
    }

    fn compare_col_value(
        src_col_value: &ColValue,
        dst_col_value: &ColValue,
        src_db_type: &DbType,
        dst_db_type: &DbType,
    ) -> bool {
        if src_col_value != dst_col_value {
            if src_col_value.is_nan() && dst_col_value.is_nan() {
                return true;
            }

            if src_db_type == dst_db_type {
                return false;
            }

            // different databases support different column types,
            // for example: we use Year in mysql, but INT in StarRocks,
            // so try to compare after both converted to string.
            return match src_col_value {
                // mysql 00:00:00 == foxlake 00:00:00.000000
                ColValue::Time(_) => {
                    DtNaiveTime::from_str(&src_col_value.to_string()).unwrap()
                        == DtNaiveTime::from_str(&dst_col_value.to_string()).unwrap()
                }
                ColValue::Date(_) => {
                    TimeUtil::date_from_str(&src_col_value.to_string()).unwrap()
                        == TimeUtil::date_from_str(&dst_col_value.to_string()).unwrap()
                }
                ColValue::DateTime(_) => {
                    TimeUtil::datetime_from_utc_str(&src_col_value.to_string()).unwrap()
                        == TimeUtil::datetime_from_utc_str(&dst_col_value.to_string()).unwrap()
                }
                ColValue::Timestamp(_) => {
                    TimeUtil::datetime_from_utc_str(&src_col_value.to_string()).unwrap()
                        == TimeUtil::datetime_from_utc_str(&dst_col_value.to_string()).unwrap()
                }
                _ => src_col_value.to_option_string() == dst_col_value.to_option_string(),
            };
        }
        true
    }

    pub async fn fetch_data(
        &self,
        db_tb: &(String, String),
        from: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        let (conn_pool_mysql, conn_pool_pg) = self.get_conn_pool(from);
        let data = if let Some(pool) = conn_pool_mysql {
            let db_type = self.get_db_type(from);
            RdbUtil::fetch_data_mysql_compatible(pool, db_tb, &db_type).await?
        } else if let Some(pool) = conn_pool_pg {
            RdbUtil::fetch_data_pg(pool, db_tb).await?
        } else {
            Vec::new()
        };
        Ok(data)
    }

    pub fn parse_full_tb_name(full_tb_name: &str, db_type: &DbType) -> (String, String) {
        let escape_pairs = SqlUtil::get_escape_pairs(&db_type);
        let tokens = ConfigTokenParser::parse(full_tb_name, &['.'], &escape_pairs);
        let (db, tb) = if tokens.len() > 1 {
            (tokens[0].to_string(), tokens[1].to_string())
        } else {
            (String::new(), full_tb_name.to_string())
        };

        (
            SqlUtil::unescape(&db, &escape_pairs[0]),
            SqlUtil::unescape(&tb, &escape_pairs[0]),
        )
    }

    /// get compare tbs
    pub fn get_compare_db_tbs(
        &self,
    ) -> anyhow::Result<(Vec<(String, String)>, Vec<(String, String)>)> {
        let db_type = self.get_db_type(SRC);
        let mut src_db_tbs =
            Self::get_compare_db_tbs_from_sqls(&db_type, &self.base.src_prepare_sqls)?;
        // since tables may be created/dropped in src_test.sql for ddl tests,
        // we also need to parse src_test.sql.
        src_db_tbs.extend_from_slice(&Self::get_compare_db_tbs_from_sqls(
            &db_type,
            &self.base.src_test_sqls,
        )?);

        let mut dst_db_tbs = vec![];
        for (db, tb) in src_db_tbs.iter() {
            let (dst_db, dst_tb) = self.router.get_tb_map(db, tb);
            dst_db_tbs.push((dst_db.into(), dst_tb.into()));
        }

        Ok((src_db_tbs, dst_db_tbs))
    }

    pub fn get_compare_db_tbs_from_sqls(
        db_type: &DbType,
        sqls: &Vec<String>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let mut db_tbs = vec![];
        let parser = DdlParser::new(db_type.to_owned());

        for sql in sqls.iter() {
            let sql = sql.to_lowercase().trim().to_string();
            let tokens: Vec<&str> = sql.split(" ").collect();
            if tokens[0].trim() != "create" || tokens[1].trim() != "table" {
                continue;
            }

            let ddl = parser.parse(&sql).unwrap();
            if ddl.ddl_type == DdlType::CreateTable {
                let (mut db, tb) = ddl.get_db_tb();
                if db.is_empty() {
                    db = PUBLIC.to_string();
                }
                db_tbs.push((db, tb));
            }
        }

        Ok(db_tbs)
    }

    fn get_filtered_db_tbs(&self) -> HashSet<(String, String)> {
        let mut filtered_db_tbs = HashSet::new();
        let db_type = &self.get_db_type(SRC);
        let delimiters = vec!['.'];
        let escape_pairs = SqlUtil::get_escape_pairs(db_type);
        let filtered_tbs_file = format!("{}/filtered_tbs.txt", &self.base.test_dir);

        if BaseTestRunner::check_path_exists(&filtered_tbs_file) {
            let lines = BaseTestRunner::load_file(&filtered_tbs_file);
            for line in lines.iter() {
                let db_tb = ConfigTokenParser::parse_config(line, &db_type, &delimiters).unwrap();
                if db_tb.len() == 2 {
                    let db = SqlUtil::unescape(&db_tb[0], &escape_pairs[0]);
                    let tb = SqlUtil::unescape(&db_tb[1], &escape_pairs[0]);
                    filtered_db_tbs.insert((db, tb));
                }
            }
        }
        filtered_db_tbs
    }

    async fn get_tb_cols(
        &self,
        db_tb: &(String, String),
        from: &str,
    ) -> anyhow::Result<Vec<String>> {
        let db_tb = self.router.get_tb_map(&db_tb.0, &db_tb.1);
        let db_tb = &(db_tb.0.into(), db_tb.1.into());

        let (conn_pool_mysql, conn_pool_pg) = self.get_conn_pool(from);
        let cols = if let Some(conn_pool) = conn_pool_mysql {
            let tb_meta = RdbUtil::get_tb_meta_mysql(conn_pool, db_tb).await?;
            tb_meta.basic.cols.clone()
        } else if let Some(conn_pool) = conn_pool_pg {
            let tb_meta = RdbUtil::get_tb_meta_pg(conn_pool, db_tb).await?;
            tb_meta.basic.cols.clone()
        } else {
            vec![]
        };
        Ok(cols)
    }

    fn get_conn_pool(&self, from: &str) -> (&Option<Pool<MySql>>, &Option<Pool<Postgres>>) {
        if from == SRC {
            (&self.src_conn_pool_mysql, &self.src_conn_pool_pg)
        } else {
            (&self.dst_conn_pool_mysql, &self.dst_conn_pool_pg)
        }
    }

    pub fn get_db_type(&self, from: &str) -> DbType {
        let config = TaskConfig::new(&self.base.task_config_file).unwrap();
        if from == SRC {
            config.extractor_basic.db_type
        } else {
            config.sinker_basic.db_type
        }
    }
}
