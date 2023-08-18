use std::collections::HashSet;

use dt_common::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser,
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    utils::{sql_util::SqlUtil, time_util::TimeUtil},
};
use dt_connector::rdb_query_builder::RdbQueryBuilder;
use dt_meta::{
    ddl_type::DdlType, mysql::mysql_meta_manager::MysqlMetaManager,
    pg::pg_meta_manager::PgMetaManager, sql_parser::ddl_parser::DdlParser,
};
use dt_task::{extractor_util::ExtractorUtil, task_util::TaskUtil};
use futures::TryStreamExt;
use sqlx::{MySql, Pool, Postgres, Row};

use crate::test_config_util::TestConfigUtil;

use super::base_test_runner::BaseTestRunner;

pub struct RdbTestRunner {
    pub base: BaseTestRunner,
    pub src_conn_pool_mysql: Option<Pool<MySql>>,
    pub dst_conn_pool_mysql: Option<Pool<MySql>>,
    pub src_conn_pool_pg: Option<Pool<Postgres>>,
    pub dst_conn_pool_pg: Option<Pool<Postgres>>,
}

pub const SRC: &str = "src";
pub const DST: &str = "dst";
pub const PUBLIC: &str = "public";

#[allow(dead_code)]
impl RdbTestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        let base = BaseTestRunner::new(relative_test_dir).await.unwrap();

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

        if dst_db_type == DbType::Mysql {
            dst_conn_pool_mysql = Some(TaskUtil::create_mysql_conn_pool(&dst_url, 1, false).await?);
        } else {
            dst_conn_pool_pg = Some(TaskUtil::create_pg_conn_pool(&dst_url, 1, false).await?);
        }

        Ok(Self {
            src_conn_pool_mysql,
            dst_conn_pool_mysql,
            src_conn_pool_pg,
            dst_conn_pool_pg,
            base,
        })
    }

    fn parse_conn_info(base: &BaseTestRunner) -> (DbType, String, DbType, String) {
        let mut src_db_type = DbType::Mysql;
        let mut src_url = String::new();
        let mut dst_db_type = DbType::Mysql;
        let mut dst_url = String::new();

        let config = TaskConfig::new(&base.task_config_file);
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

            ExtractorConfig::Basic { url, db_type, .. } => match db_type {
                DbType::Mysql => {
                    src_url = url.clone();
                }
                DbType::Pg => {
                    src_db_type = DbType::Pg;
                    src_url = url.clone();
                }
                _ => {}
            },

            _ => {}
        }

        match config.sinker {
            SinkerConfig::Mysql { url, .. }
            | SinkerConfig::MysqlCheck { url, .. }
            | SinkerConfig::MysqlStruct { url, .. } => {
                dst_url = url.clone();
            }

            SinkerConfig::Pg { url, .. }
            | SinkerConfig::PgCheck { url, .. }
            | SinkerConfig::PgStruct { url, .. } => {
                dst_db_type = DbType::Pg;
                dst_url = url.clone();
            }

            SinkerConfig::Basic { url, db_type, .. } => match db_type {
                DbType::Mysql => {
                    dst_url = url.clone();
                }
                DbType::Pg => {
                    dst_db_type = DbType::Pg;
                    dst_url = url.clone();
                }
                _ => {}
            },

            _ => {}
        }

        (src_db_type, src_url, dst_db_type, dst_url)
    }

    pub async fn run_snapshot_test(&self, compare_data: bool) -> Result<(), Error> {
        // prepare src and dst tables
        self.execute_test_ddl_sqls().await?;
        self.execute_test_dml_sqls().await?;

        // start task
        self.base.start_task().await?;

        // compare data
        let db_tbs = self.get_compare_db_tbs().await?;
        if compare_data {
            assert!(
                self.compare_data_for_tbs(&db_tbs.clone(), &db_tbs.clone())
                    .await?
            )
        }
        Ok(())
    }

    pub async fn run_ddl_test(&self, start_millis: u64, parse_millis: u64) -> Result<(), Error> {
        self.execute_test_ddl_sqls().await?;
        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        self.execute_src_sqls(&self.base.src_dml_sqls).await?;
        TimeUtil::sleep_millis(parse_millis).await;

        let db_tbs = self.get_compare_db_tbs().await?;
        assert!(
            self.compare_data_for_tbs(&db_tbs.clone(), &db_tbs.clone())
                .await?
        );
        self.base.wait_task_finish(&task).await
    }

    pub async fn run_cdc_test(&self, start_millis: u64, parse_millis: u64) -> Result<(), Error> {
        // prepare src and dst tables
        self.execute_test_ddl_sqls().await?;

        // start task
        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        // load dml sqls
        let mut src_insert_sqls = Vec::new();
        let mut src_update_sqls = Vec::new();
        let mut src_delete_sqls = Vec::new();

        for sql in self.base.src_dml_sqls.iter() {
            if sql.to_lowercase().starts_with("insert") {
                src_insert_sqls.push(sql.clone());
            } else if sql.to_lowercase().starts_with("update") {
                src_update_sqls.push(sql.clone());
            } else {
                src_delete_sqls.push(sql.clone());
            }
        }

        // insert src data
        self.execute_src_sqls(&src_insert_sqls).await?;
        TimeUtil::sleep_millis(start_millis).await;

        let db_tbs = self.get_compare_db_tbs().await?;
        let src_db_tbs = db_tbs.clone();
        let dst_db_tbs = db_tbs.clone();
        assert!(self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?);

        // update src data
        self.execute_src_sqls(&src_update_sqls).await?;
        TimeUtil::sleep_millis(parse_millis).await;
        assert!(self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?);

        // delete src data
        self.execute_src_sqls(&src_delete_sqls).await?;
        TimeUtil::sleep_millis(parse_millis).await;
        assert!(self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?);

        self.base.wait_task_finish(&task).await
    }

    pub async fn run_cdc_test_with_different_configs(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> Result<(), Error> {
        let configs = TestConfigUtil::get_default_configs();
        for config in configs {
            TestConfigUtil::update_task_config(
                &self.base.task_config_file,
                &self.base.task_config_file,
                &config,
            );
            println!("----------------------------------------");
            println!("running cdc test with config: {:?}", config);
            self.run_cdc_test(start_millis, parse_millis).await.unwrap();
        }
        Ok(())
    }

    pub async fn run_foxlake_test(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> Result<(), Error> {
        // execute src ddl sqls
        self.execute_test_ddl_sqls().await?;

        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        // execute src dml sqls
        self.execute_src_sqls(&self.base.src_dml_sqls).await?;
        TimeUtil::sleep_millis(parse_millis).await;

        self.base.wait_task_finish(&task).await
    }

    pub async fn execute_test_dml_sqls(&self) -> Result<(), Error> {
        self.execute_src_sqls(&self.base.src_dml_sqls).await?;
        self.execute_dst_sqls(&self.base.dst_dml_sqls).await
    }

    pub async fn execute_test_ddl_sqls(&self) -> Result<(), Error> {
        self.execute_src_sqls(&self.base.src_ddl_sqls).await?;
        self.execute_dst_sqls(&self.base.dst_ddl_sqls).await
    }

    pub async fn execute_clean_sqls(&self) -> Result<(), Error> {
        if TestConfigUtil::should_do_clean_or_not() {
            self.execute_src_sqls(&self.base.src_clean_sqls).await?;
            self.execute_dst_sqls(&self.base.dst_clean_sqls).await
        } else {
            Ok(())
        }
    }

    async fn execute_src_sqls(&self, sqls: &Vec<String>) -> Result<(), Error> {
        if let Some(pool) = &self.src_conn_pool_mysql {
            Self::execute_sqls_mysql(sqls, pool).await?;
        }
        if let Some(pool) = &self.src_conn_pool_pg {
            Self::execute_sqls_pg(sqls, pool).await?;
        }
        Ok(())
    }

    async fn execute_dst_sqls(&self, sqls: &Vec<String>) -> Result<(), Error> {
        if let Some(pool) = &self.dst_conn_pool_mysql {
            Self::execute_sqls_mysql(sqls, pool).await?;
        }
        if let Some(pool) = &self.dst_conn_pool_pg {
            Self::execute_sqls_pg(sqls, pool).await?;
        }
        Ok(())
    }

    async fn compare_data_for_tbs(
        &self,
        src_db_tbs: &Vec<(String, String)>,
        dst_db_tbs: &Vec<(String, String)>,
    ) -> Result<bool, Error> {
        let filtered_tbs_file = format!("{}/filtered_tbs.txt", &self.base.test_dir);
        let filtered_tbs = if BaseTestRunner::check_file_exists(&filtered_tbs_file) {
            BaseTestRunner::load_file(&filtered_tbs_file)
        } else {
            Vec::new()
        };

        for i in 0..src_db_tbs.len() {
            if filtered_tbs.contains(&format!("{}.{}", &src_db_tbs[i].0, &src_db_tbs[i].1)) {
                let dst_data = self.fetch_data(&dst_db_tbs[i], DST).await?;
                if dst_data.len() > 0 {
                    return Ok(false);
                }
            } else {
                if !self.compare_tb_data(&src_db_tbs[i], &dst_db_tbs[i]).await? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    async fn compare_tb_data(
        &self,
        src_db_tb: &(String, String),
        dst_db_tb: &(String, String),
    ) -> Result<bool, Error> {
        let src_data = self.fetch_data(src_db_tb, SRC).await?;
        let dst_data = self.fetch_data(dst_db_tb, DST).await?;
        println!(
            "comparing row data for src_tb: {:?}, src_data count: {}",
            src_db_tb,
            src_data.len()
        );

        let cols = self.get_tb_cols(src_db_tb).await?;
        if !Self::compare_row_data(&cols, &src_data, &dst_data) {
            println!(
                "compare tb data failed, src_tb: {:?}, dst_tb: {:?}",
                src_db_tb, dst_db_tb,
            );
            return Ok(false);
        }
        Ok(true)
    }

    fn compare_row_data(
        cols: &Vec<String>,
        src_data: &Vec<Vec<Option<Vec<u8>>>>,
        dst_data: &Vec<Vec<Option<Vec<u8>>>>,
    ) -> bool {
        assert_eq!(src_data.len(), dst_data.len());
        for i in 0..src_data.len() {
            let src_row_data = &src_data[i];
            let dst_row_data = &dst_data[i];

            for j in 0..src_row_data.len() {
                let src_col_value = &src_row_data[j];
                let dst_col_value = &dst_row_data[j];
                if src_col_value != dst_col_value {
                    println!(
                        "row index: {}, col: {}, src_col_value: {:?}, dst_col_value: {:?}",
                        i, cols[j], src_col_value, dst_col_value
                    );
                    return false;
                }
            }
        }
        true
    }

    pub async fn fetch_data(
        &self,
        db_tb: &(String, String),
        from: &str,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, Error> {
        let conn_pool_mysql = if from == SRC {
            &self.src_conn_pool_mysql
        } else {
            &self.dst_conn_pool_mysql
        };

        let conn_pool_pg = if from == SRC {
            &self.src_conn_pool_pg
        } else {
            &self.dst_conn_pool_pg
        };

        let data = if let Some(pool) = conn_pool_mysql {
            self.fetch_data_mysql(db_tb, pool).await?
        } else if let Some(pool) = conn_pool_pg {
            self.fetch_data_pg(db_tb, pool).await?
        } else {
            Vec::new()
        };
        Ok(data)
    }

    pub fn parse_full_tb_name(full_tb_name: &str, db_type: &DbType) -> (String, String) {
        let escape_pairs = SqlUtil::get_escape_pairs(&db_type);
        let tokens = ConfigTokenParser::parse(full_tb_name, &vec!['.'], &escape_pairs);
        let (db, tb) = if tokens.len() > 1 {
            (tokens[0].to_string(), tokens[1].to_string())
        } else {
            ("".to_string(), full_tb_name.to_string())
        };

        (
            SqlUtil::unescape(&db, &escape_pairs[0]),
            SqlUtil::unescape(&tb, &escape_pairs[0]),
        )
    }

    async fn fetch_data_mysql(
        &self,
        db_tb: &(String, String),
        conn_pool: &Pool<MySql>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, Error> {
        let cols = self.get_tb_cols(db_tb).await?;
        let sql = format!(
            "SELECT * FROM `{}`.`{}` ORDER BY `{}`",
            &db_tb.0, &db_tb.1, &cols[0],
        );
        let query = sqlx::query(&sql);
        let mut rows = query.fetch(conn_pool);

        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let mut row_values = Vec::new();
            for col in cols.iter() {
                let value: Option<Vec<u8>> = row.get_unchecked(col.as_str());
                row_values.push(value);
            }
            result.push(row_values);
        }

        Ok(result)
    }

    async fn fetch_data_pg(
        &self,
        db_tb: &(String, String),
        conn_pool: &Pool<Postgres>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, Error> {
        let mut meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;
        let tb_meta = meta_manager.get_tb_meta(&db_tb.0, &db_tb.1).await?;
        let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta);

        let cols = self.get_tb_cols(&db_tb).await?;
        let cols_str = query_builder.build_extract_cols_str().unwrap();
        let sql = format!(
            r#"SELECT {} FROM "{}"."{}" ORDER BY "{}" ASC"#,
            cols_str, &db_tb.0, &db_tb.1, &cols[0],
        );
        let query = sqlx::query(&sql);
        let mut rows = query.fetch(conn_pool);

        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let mut row_values = Vec::new();
            for col in cols.iter() {
                let value: Option<Vec<u8>> = row.get_unchecked(col.as_str());
                row_values.push(value);
            }
            result.push(row_values);
        }

        Ok(result)
    }

    async fn execute_sqls_mysql(sqls: &Vec<String>, conn_pool: &Pool<MySql>) -> Result<(), Error> {
        for sql in sqls {
            println!("executing sql: {}", sql);
            let query = sqlx::query(sql);
            query.execute(conn_pool).await.unwrap();
        }
        Ok(())
    }

    async fn execute_sqls_pg(sqls: &Vec<String>, conn_pool: &Pool<Postgres>) -> Result<(), Error> {
        for sql in sqls.iter() {
            println!("executing sql: {}", sql);
            let query = sqlx::query(sql);
            query.execute(conn_pool).await.unwrap();
        }
        Ok(())
    }

    /// get compare dbs from src_ddl_sqls
    async fn get_compare_dbs(&self, url: &str, db_type: &DbType) -> Result<HashSet<String>, Error> {
        let all_dbs = ExtractorUtil::list_dbs(url, db_type).await?;
        let mut dbs = HashSet::new();
        for sql in self.base.src_ddl_sqls.iter() {
            if !sql.to_lowercase().contains("database") {
                continue;
            }
            let ddl = DdlParser::parse(sql).unwrap();
            if ddl.0 == DdlType::DropDatabase || ddl.0 == DdlType::CreateDatabase {
                let db = ddl.1.unwrap();
                // db may be dropped in test sql (ddl test)
                if all_dbs.contains(&db) {
                    dbs.insert(db);
                }
            }
        }
        Ok(dbs)
    }

    /// get compare tbs
    async fn get_compare_db_tbs(&self) -> Result<Vec<(String, String)>, Error> {
        let mut db_tbs = vec![];
        let (src_db_type, src_url, _, _) = Self::parse_conn_info(&self.base);

        if src_db_type == DbType::Mysql {
            let dbs = self.get_compare_dbs(&src_url, &src_db_type).await?;
            for db in dbs.iter() {
                let tbs = ExtractorUtil::list_tbs(&src_url, db, &src_db_type).await?;
                for tb in tbs.iter() {
                    db_tbs.push((db.to_string(), tb.to_string()));
                }
            }
        } else {
            for sql in self.base.src_ddl_sqls.iter() {
                if !sql.to_lowercase().contains("table") {
                    continue;
                }
                let ddl = DdlParser::parse(sql).unwrap();
                if ddl.0 == DdlType::CreateTable {
                    let db = if let Some(db) = ddl.1 {
                        db
                    } else {
                        PUBLIC.to_string()
                    };
                    let tb = ddl.2.unwrap();
                    db_tbs.push((db, tb));
                }
            }
        }
        Ok(db_tbs)
    }

    async fn get_tb_cols(&self, db_tb: &(String, String)) -> Result<Vec<String>, Error> {
        let cols = if let Some(conn_pool) = &self.src_conn_pool_mysql {
            let mut meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;
            let tb_meta = meta_manager.get_tb_meta(&db_tb.0, &db_tb.1).await?;
            tb_meta.basic.cols.clone()
        } else if let Some(conn_pool) = &self.src_conn_pool_pg {
            let mut meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;
            let tb_meta = meta_manager.get_tb_meta(&db_tb.0, &db_tb.1).await?;
            tb_meta.basic.cols.clone()
        } else {
            vec![]
        };
        Ok(cols)
    }
}
