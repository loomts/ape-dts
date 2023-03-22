use futures::TryStreamExt;
use sqlx::{MySql, Pool, Postgres, Row};
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{BufRead, BufReader},
    thread,
    time::Duration,
};

use crate::{
    config::{
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    task::{task_runner::TaskRunner, task_util::TaskUtil},
};

use super::test_config_util::TestConfigUtil;

pub struct TestRunner {
    test_dir: String,
    log_dir: String,
    task_config_file: String,
    src_conn_pool_mysql: Option<Pool<MySql>>,
    dst_conn_pool_mysql: Option<Pool<MySql>>,
    src_conn_pool_pg: Option<Pool<Postgres>>,
    dst_conn_pool_pg: Option<Pool<Postgres>>,
    src_ddl_file: String,
    dst_ddl_file: String,
    src_dml_file: String,
    dst_dml_file: String,
}

static mut LOG4RS_INITED: bool = false;

#[allow(dead_code)]
impl TestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        let project_root = TestConfigUtil::get_project_root();
        let tmp_dir = format!("{}/tmp", project_root);
        let test_dir = TestConfigUtil::get_absolute_dir(relative_test_dir);
        let src_task_config_file = format!("{}/task_config.ini", test_dir);
        let dst_task_config_file = format!("{}/task_config.ini", tmp_dir);

        // update relative path to absolute path in task_config.ini
        let log_dir = format!("{}/logs", project_root);
        let check_log_dir = format!("{}/check_log", test_dir);
        let config = vec![
            (
                "extractor".to_string(),
                "check_log_dir".to_string(),
                check_log_dir,
            ),
            (
                "runtime".to_string(),
                "log_dir".to_string(),
                log_dir.clone(),
            ),
        ];
        TestConfigUtil::update_task_config(&src_task_config_file, &dst_task_config_file, &config);

        let src_ddl_file = format!("{}/src_ddl.sql", test_dir);
        let dst_ddl_file = format!("{}/dst_ddl.sql", test_dir);
        let src_dml_file = format!("{}/src_dml.sql", test_dir);
        let dst_dml_file = format!("{}/dst_dml.sql", test_dir);

        let mut src_conn_pool_mysql = None;
        let mut dst_conn_pool_mysql = None;
        let mut src_conn_pool_pg = None;
        let mut dst_conn_pool_pg = None;

        let config = TaskConfig::new(&dst_task_config_file);
        match config.extractor {
            ExtractorConfig::MysqlCdc { url, .. }
            | ExtractorConfig::MysqlSnapshot { url, .. }
            | ExtractorConfig::MysqlCheck { url, .. } => {
                src_conn_pool_mysql = Some(TaskUtil::create_mysql_conn_pool(&url, 1, true).await?);
            }

            ExtractorConfig::PgCdc { url, .. }
            | ExtractorConfig::PgSnapshot { url, .. }
            | ExtractorConfig::PgCheck { url, .. } => {
                src_conn_pool_pg = Some(TaskUtil::create_pg_conn_pool(&url, 1, true).await?);
            }
        }

        match config.sinker {
            SinkerConfig::Mysql { url, .. } | SinkerConfig::MysqlCheck { url, .. } => {
                dst_conn_pool_mysql = Some(TaskUtil::create_mysql_conn_pool(&url, 1, true).await?);
            }

            SinkerConfig::Pg { url, .. } | SinkerConfig::PgCheck { url, .. } => {
                dst_conn_pool_pg = Some(TaskUtil::create_pg_conn_pool(&url, 1, true).await?);
            }
        }

        Ok(Self {
            src_conn_pool_mysql,
            dst_conn_pool_mysql,
            src_conn_pool_pg,
            dst_conn_pool_pg,
            src_ddl_file,
            dst_ddl_file,
            src_dml_file,
            dst_dml_file,
            task_config_file: dst_task_config_file,
            test_dir,
            log_dir,
        })
    }

    pub async fn run_cdc_test_with_different_configs(
        &self,
        start_millis: u64,
        parse_millis: u64,
        configs: &Vec<Vec<(String, String, String)>>,
    ) -> Result<(), Error> {
        for config in configs {
            TestConfigUtil::update_task_config(
                &self.task_config_file,
                &self.task_config_file,
                config,
            );
            println!("----------------------------------------");
            println!("running cdc test with config: {:?}", config);
            self.run_cdc_test(start_millis, parse_millis).await.unwrap();
        }
        Ok(())
    }

    pub async fn run_check_test(&self) -> Result<(), Error> {
        // clear existed check logs
        Self::clear_check_log(&self.log_dir);
        self.run_test_ddl_sqls().await?;
        self.run_test_dml_sqls().await?;

        // start task
        self.start_task().await?;

        // check result
        let expect_log_dir = format!("{}/expect_check_log", self.test_dir);
        let actual_log_dir = format!("{}/check", self.log_dir);
        let (expect_miss_logs, expect_diff_logs) = Self::load_check_log(&expect_log_dir);
        let (actual_miss_logs, actual_diff_logs) = Self::load_check_log(&actual_log_dir);

        assert_eq!(expect_diff_logs.len(), actual_miss_logs.len());
        assert_eq!(expect_miss_logs.len(), actual_miss_logs.len());
        for log in expect_diff_logs {
            assert!(actual_diff_logs.contains(&log))
        }
        for log in expect_miss_logs {
            assert!(actual_miss_logs.contains(&log))
        }

        Ok(())
    }

    pub async fn run_revise_test(&self) -> Result<(), Error> {
        Self::clear_check_log(&self.log_dir);
        self.run_snapshot_test().await
    }

    pub async fn run_review_test(&self) -> Result<(), Error> {
        Self::clear_check_log(&self.log_dir);
        self.run_check_test().await
    }

    pub async fn run_snapshot_test(&self) -> Result<(), Error> {
        let (src_tbs, dst_tbs, cols_list) = TestRunner::parse_ddl(&self.src_ddl_file).unwrap();

        // prepare src and dst tables
        self.run_test_ddl_sqls().await?;
        self.run_test_dml_sqls().await?;

        // start task
        self.start_task().await?;
        assert!(
            self.compare_data_for_tbs(&src_tbs, &dst_tbs, &cols_list)
                .await?
        );
        Ok(())
    }

    pub async fn run_cdc_test(&self, start_millis: u64, parse_millis: u64) -> Result<(), Error> {
        let (src_tbs, dst_tbs, cols_list) = Self::parse_ddl(&self.src_ddl_file).unwrap();

        // prepare src and dst tables
        self.run_test_ddl_sqls().await?;

        // start task
        let enable_log4rs = Self::get_enable_log4rs();
        let task_runner = TaskRunner::new(self.task_config_file.clone()).await;
        let task =
            tokio::spawn(async move { task_runner.start_task(enable_log4rs).await.unwrap() });
        TaskUtil::sleep_millis(start_millis).await;

        // load dml sqls
        let src_dml_file_sqls = TestRunner::load_sqls(&self.src_dml_file);
        let mut src_insert_sqls = Vec::new();
        let mut src_update_sqls = Vec::new();
        let mut src_delete_sqls = Vec::new();

        for sql in src_dml_file_sqls {
            if sql.to_lowercase().starts_with("insert") {
                src_insert_sqls.push(sql);
            } else if sql.to_lowercase().starts_with("update") {
                src_update_sqls.push(sql);
            } else {
                src_delete_sqls.push(sql);
            }
        }

        // insert src data
        self.execute_src_sqls(&src_insert_sqls).await?;
        thread::sleep(Duration::from_millis(parse_millis));
        assert!(
            self.compare_data_for_tbs(&src_tbs, &dst_tbs, &cols_list)
                .await?
        );

        // update src data
        self.execute_src_sqls(&src_update_sqls).await?;
        TaskUtil::sleep_millis(parse_millis).await;
        assert!(
            self.compare_data_for_tbs(&src_tbs, &dst_tbs, &cols_list)
                .await?
        );

        // delete src data
        self.execute_src_sqls(&src_delete_sqls).await?;
        TaskUtil::sleep_millis(parse_millis).await;
        assert!(
            self.compare_data_for_tbs(&src_tbs, &dst_tbs, &cols_list)
                .await?
        );

        task.abort();
        while !task.is_finished() {
            TaskUtil::sleep_millis(1).await;
        }

        Ok(())
    }

    async fn start_task(&self) -> Result<(), Error> {
        let enable_log4rs = Self::get_enable_log4rs();
        let task_config = self.task_config_file.clone();
        TaskRunner::new(task_config)
            .await
            .start_task(enable_log4rs)
            .await
    }

    fn get_enable_log4rs() -> bool {
        // all tests will be run in one process, and log4rs can only be inited once
        let enable_log4rs = if unsafe { LOG4RS_INITED } {
            false
        } else {
            true
        };

        if enable_log4rs {
            unsafe { LOG4RS_INITED = true };
        }
        enable_log4rs
    }

    pub fn parse_ddl(
        src_ddl_file: &str,
    ) -> Result<(Vec<String>, Vec<String>, Vec<Vec<String>>), Error> {
        let src_ddl_sqls = TestRunner::load_sqls(src_ddl_file);
        let mut src_tbs = Vec::new();
        let mut cols_list = Vec::new();
        for sql in src_ddl_sqls {
            if sql.to_lowercase().contains("create table") {
                let (tb, cols) = TestRunner::parse_create_table(&sql).unwrap();
                src_tbs.push(tb);
                cols_list.push(cols);
            }
        }
        let dst_tbs = src_tbs.clone();

        Ok((src_tbs, dst_tbs, cols_list))
    }

    pub fn parse_create_table(sql: &str) -> Result<(String, Vec<String>), Error> {
        // since both (sqlparser = "0.30.0", sql-parse = "0.11.0") have bugs in parsing create table sql,
        // we take a simple workaround to get table name and column names.

        // CREATE TABLE bitbin_table (pk SERIAL, bvunlimited1 BIT VARYING, p GEOMETRY(POINT,3187), PRIMARY KEY(pk));
        let mut tokens = Vec::new();
        let mut token = "".to_string();
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

    fn load_file(file_path: &str) -> Vec<String> {
        let file = File::open(file_path).unwrap();
        let reader = BufReader::new(file);

        let mut lines = Vec::new();
        for line in reader.lines() {
            if let Ok(str) = line {
                lines.push(str);
            }
        }
        lines
    }

    fn load_check_log(log_dir: &str) -> (HashSet<String>, HashSet<String>) {
        let miss_log_file = format!("{}/miss.log", log_dir);
        let diff_log_file = format!("{}/diff.log", log_dir);
        let mut miss_logs = HashSet::new();
        let mut diff_logs = HashSet::new();

        for log in Self::load_file(&miss_log_file) {
            miss_logs.insert(log);
        }
        for log in Self::load_file(&diff_log_file) {
            diff_logs.insert(log);
        }
        (miss_logs, diff_logs)
    }

    fn clear_check_log(log_dir: &str) {
        let miss_log_file = format!("{}/check/miss.log", log_dir);
        let diff_log_file = format!("{}/check/diff.log", log_dir);
        if fs::metadata(&miss_log_file).is_ok() {
            File::create(&miss_log_file).unwrap().set_len(0).unwrap();
        }
        if fs::metadata(&diff_log_file).is_ok() {
            File::create(&diff_log_file).unwrap().set_len(0).unwrap();
        }
    }

    fn load_sqls(sql_file: &str) -> Vec<String> {
        let mut lines = Self::load_file(sql_file);
        let mut sqls = Vec::new();
        for sql in lines.drain(..) {
            let sql = sql.trim();
            if sql.is_empty() || sql.starts_with("--") {
                continue;
            }
            sqls.push(sql.to_string());
        }
        sqls
    }

    fn load_perf_dml_sqls(
        sql_file: &str,
        batch_size: u32,
    ) -> Result<(Vec<String>, Vec<u32>), Error> {
        let lines = Self::load_sqls(sql_file);
        let mut perf_sqls = Vec::new();
        let mut counts = Vec::new();

        let mut i = 0;
        while i < lines.len() {
            let sql = lines[i].replace("insert_sql:", "");
            let value = lines[i + 1].replace("insert_value:", "");
            let count = lines[i + 2]
                .replace("insert_count:", "")
                .trim()
                .parse::<u32>()
                .unwrap();
            i += 3;

            let mut values = Vec::new();
            for _ in 0..batch_size {
                values.push(value.clone());
            }

            let perf_sql = format!("{} VALUES {}", sql, values.join(","));
            perf_sqls.push(perf_sql);
            counts.push(count / batch_size);
        }
        Ok((perf_sqls, counts))
    }

    async fn run_test_dml_sqls(&self) -> Result<(), Error> {
        if fs::metadata(&self.src_dml_file).is_ok() {
            let src_dml_file_sqls = Self::load_sqls(&self.src_dml_file);
            self.execute_src_sqls(&src_dml_file_sqls).await?;
        }

        if fs::metadata(&self.dst_dml_file).is_ok() {
            let dst_dml_file_sqls = Self::load_sqls(&self.dst_dml_file);
            self.execute_dst_sqls(&dst_dml_file_sqls).await?;
        }
        Ok(())
    }

    async fn run_test_ddl_sqls(&self) -> Result<(), Error> {
        let src_ddl_sqls = Self::load_sqls(&self.src_ddl_file);
        let dst_ddl_sqls = Self::load_sqls(&self.dst_ddl_file);

        if let Some(pool) = &self.src_conn_pool_mysql {
            Self::execute_sqls_mysql(&src_ddl_sqls, pool).await?;
        }

        if let Some(pool) = &self.src_conn_pool_pg {
            Self::execute_sqls_pg(&src_ddl_sqls, pool).await?;
        }

        if let Some(pool) = &self.dst_conn_pool_mysql {
            Self::execute_sqls_mysql(&dst_ddl_sqls, pool).await?;
        }

        if let Some(pool) = &self.dst_conn_pool_pg {
            Self::execute_sqls_pg(&dst_ddl_sqls, pool).await?;
        }

        Ok(())
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
        for i in 0..src_tbs.len() {
            let src_tb = &src_tbs[i];
            let dst_tb = &dst_tbs[i];
            if !self.compare_tb_data(src_tb, dst_tb, &cols_list[i]).await? {
                return Ok(false);
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
        let src_data = if let Some(pool) = &self.src_conn_pool_mysql {
            self.fetch_data_mysql(src_tb, cols, pool).await?
        } else if let Some(pool) = &self.src_conn_pool_pg {
            self.fetch_data_pg(src_tb, cols, pool).await?
        } else {
            Vec::new()
        };

        let dst_data = if let Some(pool) = &self.dst_conn_pool_mysql {
            self.fetch_data_mysql(dst_tb, cols, pool).await?
        } else if let Some(pool) = &self.dst_conn_pool_pg {
            self.fetch_data_pg(dst_tb, cols, pool).await?
        } else {
            Vec::new()
        };

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

    async fn fetch_data_mysql(
        &self,
        tb: &str,
        cols: &Vec<String>,
        conn_pool: &Pool<MySql>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, Error> {
        let sql = format!("SELECT * FROM {} ORDER BY {}", tb, cols[0]);
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
        tb: &str,
        cols: &Vec<String>,
        conn_pool: &Pool<Postgres>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, Error> {
        let mut extract_cols = Vec::new();
        for col in cols {
            extract_cols.push(format!("{}::text", col));
        }
        let sql = format!(
            "SELECT {} FROM {} ORDER BY {} ASC",
            extract_cols.join(","),
            tb,
            cols[0]
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
}
