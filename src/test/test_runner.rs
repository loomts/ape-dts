use futures::TryStreamExt;
use sqlx::{MySql, Pool, Postgres, Row};
use std::{env, fs::File, io::Read, thread, time::Duration};

use crate::{
    config::{
        config_loader::ConfigLoader, extractor_config::ExtractorConfig, sinker_config::SinkerConfig,
    },
    error::Error,
    task::{task_runner::TaskRunner, task_util::TaskUtil},
};

pub struct TestRunner {
    src_conn_pool_mysql: Option<Pool<MySql>>,
    dst_conn_pool_mysql: Option<Pool<MySql>>,
    src_conn_pool_pg: Option<Pool<Postgres>>,
    dst_conn_pool_pg: Option<Pool<Postgres>>,
    src_ddl_file: String,
    dst_ddl_file: String,
    src_dml_file: String,
    task_config_file: String,
}

#[allow(dead_code)]
impl TestRunner {
    pub async fn new(test_dir: &str) -> Result<Self, Error> {
        let src_ddl_file = format!("{}/src_ddl.sql", test_dir);
        let dst_ddl_file = format!("{}/dst_ddl.sql", test_dir);
        let src_dml_file = format!("{}/src_dml.sql", test_dir);
        let task_config_file = format!("{}/task_config.ini", test_dir);

        let mut src_conn_pool_mysql = None;
        let mut dst_conn_pool_mysql = None;
        let mut src_conn_pool_pg = None;
        let mut dst_conn_pool_pg = None;

        let (extractor_config, sinker_config, _, _, _) = ConfigLoader::load(&task_config_file)?;

        match extractor_config {
            ExtractorConfig::MysqlCdc { url, .. } | ExtractorConfig::MysqlSnapshot { url, .. } => {
                src_conn_pool_mysql = Some(TaskUtil::create_mysql_conn_pool(&url, 1, true).await?);
            }
            ExtractorConfig::PgCdc { url, .. } | ExtractorConfig::PgSnapshot { url, .. } => {
                src_conn_pool_pg = Some(TaskUtil::create_pg_conn_pool(&url, 1, true).await?);
            }
        }

        match sinker_config {
            SinkerConfig::Mysql { url, .. } => {
                dst_conn_pool_mysql = Some(TaskUtil::create_mysql_conn_pool(&url, 1, true).await?);
            }
            SinkerConfig::Pg { url, .. } => {
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
            task_config_file,
        })
    }

    pub async fn run_snapshot_test(&self) -> Result<(), Error> {
        let (src_tbs, dst_tbs, cols_list) = TestRunner::parse_ddl(&self.src_ddl_file).unwrap();

        // prepare src and dst tables
        self.prepare_test_tbs(&self.src_ddl_file, &self.dst_ddl_file)
            .await?;

        // prepare src data
        let src_dml_file_sqls = TestRunner::load_sqls(&self.src_dml_file)?;
        self.execute_src_sqls(&src_dml_file_sqls).await?;

        // start task
        TaskRunner::start_task(self.task_config_file.clone()).await?;
        assert!(
            self.compare_data_for_tbs(&src_tbs, &dst_tbs, &cols_list)
                .await?
        );
        Ok(())
    }

    pub async fn run_cdc_test(&self, start_millis: u64, parse_millis: u64) -> Result<(), Error> {
        let (src_tbs, dst_tbs, cols_list) = Self::parse_ddl(&self.src_ddl_file).unwrap();

        // prepare src and dst tables
        self.prepare_test_tbs(&self.src_ddl_file, &self.dst_ddl_file)
            .await?;

        // start task
        let task_config = self.task_config_file.clone();
        tokio::spawn(async move { TaskRunner::start_task(task_config).await });
        TaskUtil::sleep_millis(start_millis).await;

        // load dml sqls
        let src_dml_file_sqls = TestRunner::load_sqls(&self.src_dml_file)?;
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

        Ok(())
    }

    pub fn parse_ddl(
        src_ddl_file: &str,
    ) -> Result<(Vec<String>, Vec<String>, Vec<Vec<String>>), Error> {
        let src_ddl_sqls = TestRunner::load_sqls(src_ddl_file).unwrap();
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

    pub fn load_sqls(sql_file: &str) -> Result<Vec<String>, Error> {
        let mut content = String::new();
        let file_path = env::current_dir().unwrap().join(sql_file);
        File::open(file_path)?.read_to_string(&mut content)?;

        let mut sqls = Vec::new();
        for mut sql in content.split("\n") {
            sql = sql.trim();
            if sql.is_empty() || sql.starts_with("--") {
                continue;
            }
            sqls.push(sql.to_string());
        }
        Ok(sqls)
    }

    pub async fn prepare_test_tbs(
        &self,
        src_ddl_file: &str,
        dst_ddl_file: &str,
    ) -> Result<(), Error> {
        let src_ddl_sqls = Self::load_sqls(src_ddl_file)?;
        let dst_ddl_sqls = Self::load_sqls(dst_ddl_file)?;
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

    pub async fn execute_src_sqls(&self, sqls: &Vec<String>) -> Result<(), Error> {
        if let Some(pool) = &self.src_conn_pool_mysql {
            Self::execute_sqls_mysql(sqls, pool).await?;
        }
        if let Some(pool) = &self.src_conn_pool_pg {
            Self::execute_sqls_pg(sqls, pool).await?;
        }
        Ok(())
    }

    pub async fn compare_data_for_tbs(
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

    pub async fn compare_tb_data(
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
