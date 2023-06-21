use dt_common::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser,
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    utils::{sql_util::SqlUtil, time_util::TimeUtil},
};
use dt_connector::rdb_query_builder::RdbQueryBuilder;
use dt_meta::{mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager};
use dt_task::task_util::TaskUtil;
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

#[allow(dead_code)]
impl RdbTestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        let base = BaseTestRunner::new(relative_test_dir).await.unwrap();

        // prepare conn pools
        let mut src_conn_pool_mysql = None;
        let mut dst_conn_pool_mysql = None;
        let mut src_conn_pool_pg = None;
        let mut dst_conn_pool_pg = None;

        let config = TaskConfig::new(&base.task_config_file);
        match config.extractor {
            ExtractorConfig::MysqlCdc { url, .. }
            | ExtractorConfig::MysqlSnapshot { url, .. }
            | ExtractorConfig::MysqlCheck { url, .. }
            | ExtractorConfig::MysqlStruct { url, .. } => {
                src_conn_pool_mysql = Some(TaskUtil::create_mysql_conn_pool(&url, 1, false).await?);
            }

            ExtractorConfig::PgCdc { url, .. }
            | ExtractorConfig::PgSnapshot { url, .. }
            | ExtractorConfig::PgCheck { url, .. }
            | ExtractorConfig::PgStruct { url, .. } => {
                src_conn_pool_pg = Some(TaskUtil::create_pg_conn_pool(&url, 1, false).await?);
            }

            ExtractorConfig::Basic { url, db_type, .. } => match db_type {
                DbType::Mysql => {
                    src_conn_pool_mysql =
                        Some(TaskUtil::create_mysql_conn_pool(&url, 1, false).await?);
                }
                DbType::Pg => {
                    src_conn_pool_pg = Some(TaskUtil::create_pg_conn_pool(&url, 1, false).await?);
                }
                _ => {}
            },

            _ => {}
        }

        match config.sinker {
            SinkerConfig::Mysql { url, .. }
            | SinkerConfig::MysqlCheck { url, .. }
            | SinkerConfig::MysqlStruct { url, .. } => {
                dst_conn_pool_mysql = Some(TaskUtil::create_mysql_conn_pool(&url, 1, false).await?);
            }

            SinkerConfig::Pg { url, .. }
            | SinkerConfig::PgCheck { url, .. }
            | SinkerConfig::PgStruct { url, .. } => {
                dst_conn_pool_pg = Some(TaskUtil::create_pg_conn_pool(&url, 1, false).await?);
            }

            SinkerConfig::Basic { url, db_type, .. } => match db_type {
                DbType::Mysql => {
                    dst_conn_pool_mysql =
                        Some(TaskUtil::create_mysql_conn_pool(&url, 1, false).await?);
                }
                DbType::Pg => {
                    dst_conn_pool_pg = Some(TaskUtil::create_pg_conn_pool(&url, 1, false).await?);
                }
                _ => {}
            },

            _ => {}
        }

        Ok(Self {
            src_conn_pool_mysql,
            dst_conn_pool_mysql,
            src_conn_pool_pg,
            dst_conn_pool_pg,
            base,
        })
    }

    pub async fn run_snapshot_test(&self, compare_data: bool) -> Result<(), Error> {
        let (src_tbs, dst_tbs, cols_list) = Self::parse_ddl(&self.base.src_ddl_sqls).unwrap();

        // prepare src and dst tables
        self.execute_test_ddl_sqls().await?;
        self.execute_test_dml_sqls().await?;

        // start task
        self.base.start_task().await?;

        // compare data
        if compare_data {
            assert!(
                self.compare_data_for_tbs(&src_tbs, &dst_tbs, &cols_list)
                    .await?
            );
        }

        Ok(())
    }

    pub async fn run_cdc_test(&self, start_millis: u64, parse_millis: u64) -> Result<(), Error> {
        let (src_tbs, dst_tbs, cols_list) = Self::parse_ddl(&self.base.src_ddl_sqls).unwrap();

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
        assert!(
            self.compare_data_for_tbs(&src_tbs, &dst_tbs, &cols_list)
                .await?
        );

        // update src data
        self.execute_src_sqls(&src_update_sqls).await?;
        TimeUtil::sleep_millis(parse_millis).await;
        assert!(
            self.compare_data_for_tbs(&src_tbs, &dst_tbs, &cols_list)
                .await?
        );

        // delete src data
        self.execute_src_sqls(&src_delete_sqls).await?;
        TimeUtil::sleep_millis(parse_millis).await;
        assert!(
            self.compare_data_for_tbs(&src_tbs, &dst_tbs, &cols_list)
                .await?
        );

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
        src_tbs: &Vec<String>,
        dst_tbs: &Vec<String>,
        cols_list: &Vec<Vec<String>>,
    ) -> Result<bool, Error> {
        let filtered_tbs_file = format!("{}/filtered_tbs.txt", &self.base.test_dir);
        let filtered_tbs = if BaseTestRunner::check_file_exists(&filtered_tbs_file) {
            BaseTestRunner::load_file(&filtered_tbs_file)
        } else {
            Vec::new()
        };

        for i in 0..src_tbs.len() {
            let src_tb = &src_tbs[i];
            let dst_tb = &dst_tbs[i];
            if filtered_tbs.contains(src_tb) {
                let dst_data = self.fetch_data(&dst_tb, DST).await?;
                if dst_data.len() > 0 {
                    return Ok(false);
                }
            } else {
                if !self.compare_tb_data(src_tb, dst_tb, &cols_list[i]).await? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    async fn compare_tb_data(
        &self,
        src_tb: &str,
        dst_tb: &str,
        cols: &Vec<String>,
    ) -> Result<bool, Error> {
        let src_data = self.fetch_data(src_tb, SRC).await?;
        let dst_data = self.fetch_data(dst_tb, DST).await?;
        println!(
            "comparing row data for src_tb: {}, src_data count: {}",
            src_tb,
            src_data.len()
        );

        if !Self::compare_row_data(cols, &src_data, &dst_data) {
            println!(
                "compare_tb_data failed, src_tb: {}, dst_tb: {}",
                src_tb, dst_tb
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
                        "col: {}, src_col_value: {:?}, dst_col_value: {:?}",
                        cols[j], src_col_value, dst_col_value
                    );
                    return false;
                }
            }
        }
        true
    }

    pub async fn fetch_data(
        &self,
        full_tb_name: &str,
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
            self.fetch_data_mysql(full_tb_name, pool).await?
        } else if let Some(pool) = conn_pool_pg {
            self.fetch_data_pg(full_tb_name, pool).await?
        } else {
            Vec::new()
        };
        Ok(data)
    }

    fn parse_full_tb_name(full_tb_name: &str, db_type: DbType) -> (String, String) {
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
        full_tb_name: &str,
        conn_pool: &Pool<MySql>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, Error> {
        let (db, tb) = Self::parse_full_tb_name(full_tb_name, DbType::Mysql);
        let mut meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;
        let tb_meta = meta_manager.get_tb_meta(&db, &tb).await?;
        let cols = &tb_meta.basic.cols;

        let sql = format!("SELECT * FROM `{}`.`{}` ORDER BY `{}`", &db, &tb, &cols[0],);
        let query = sqlx::query(&sql);
        let mut rows = query.fetch(conn_pool);

        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let mut row_values = Vec::new();
            for col in cols {
                let value: Option<Vec<u8>> = row.get_unchecked(col.as_str());
                row_values.push(value);
            }
            result.push(row_values);
        }

        Ok(result)
    }

    async fn fetch_data_pg(
        &self,
        full_tb_name: &str,
        conn_pool: &Pool<Postgres>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, Error> {
        let (mut db, tb) = Self::parse_full_tb_name(full_tb_name, DbType::Pg);
        if db.is_empty() {
            db = "public".to_string();
        }
        let mut meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;
        let tb_meta = meta_manager.get_tb_meta(&db, &tb).await?;
        let cols = &tb_meta.basic.cols;
        let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta);
        let cols_str = query_builder.build_extract_cols_str().unwrap();

        let sql = format!(
            r#"SELECT {} FROM "{}"."{}" ORDER BY "{}" ASC"#,
            cols_str, &db, &tb, &cols[0],
        );
        let query = sqlx::query(&sql);
        let mut rows = query.fetch(conn_pool);

        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let mut row_values = Vec::new();
            for col in cols {
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

    fn parse_ddl(
        src_ddl_sqls: &Vec<String>,
    ) -> Result<(Vec<String>, Vec<String>, Vec<Vec<String>>), Error> {
        let mut src_tbs = Vec::new();
        let mut cols_list = Vec::new();
        for sql in src_ddl_sqls {
            if sql.to_lowercase().contains("create table") {
                let (tb, cols) = Self::parse_create_table(&sql).unwrap();
                src_tbs.push(tb);
                cols_list.push(cols);
            }
        }
        let dst_tbs = src_tbs.clone();

        Ok((src_tbs, dst_tbs, cols_list))
    }

    fn parse_create_table(sql: &str) -> Result<(String, Vec<String>), Error> {
        // since both (sqlparser = "0.30.0", sql-parse = "0.11.0") have bugs in parsing create table sql,
        // we take a simple workaround to get table name and column names.

        // CREATE TABLE bitbin_table (pk SERIAL, bvunlimited1 BIT VARYING, p GEOMETRY(POINT,3187), PRIMARY KEY(pk));
        let mut tokens = Vec::new();
        let mut token = String::new();
        let mut brakets = 0;

        for b in sql.as_bytes() {
            let c = char::from(*b);
            if c == ' ' || c == '(' || c == ')' || c == ',' {
                if !token.is_empty() {
                    tokens.push(token.clone());
                    token.clear();
                }

                if c == '(' {
                    brakets += 1;
                } else if c == ')' {
                    brakets -= 1;
                    if brakets == 0 {
                        break;
                    }
                }

                if c == ',' && brakets == 1 {
                    tokens.push(",".to_string());
                }
                continue;
            }
            token.push(c);
        }

        let tb = tokens[2].clone();
        let mut cols = vec![tokens[3].clone()];
        for i in 3..tokens.len() {
            let token_lowercase = tokens[i].to_lowercase();
            if token_lowercase == "primary"
                || token_lowercase == "unique"
                || token_lowercase == "key"
            {
                break;
            }

            if tokens[i - 1] == "," {
                cols.push(tokens[i].clone());
            }
        }

        Ok((tb, cols))
    }
}
