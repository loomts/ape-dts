use futures::TryStreamExt;
use sqlx::{MySql, Pool, Postgres, Row};
use std::{env, fs::File, io::Read};

use crate::{
    config::{
        config_loader::ConfigLoader, extractor_config::ExtractorConfig, sinker_config::SinkerConfig,
    },
    error::Error,
    task::{task_runner::TaskRunner, task_util::TaskUtil},
};

pub struct TestRunner {
    pub src_conn_pool_mysql: Option<Pool<MySql>>,
    pub dst_conn_pool_mysql: Option<Pool<MySql>>,
    pub src_conn_pool_pg: Option<Pool<Postgres>>,
    pub dst_conn_pool_pg: Option<Pool<Postgres>>,
}

#[allow(dead_code)]
impl TestRunner {
    pub fn get_default_tbs() -> Vec<String> {
        let mut tbs = Vec::new();
        for tb in vec![
            "test_db_1.no_pk_no_uk",
            "test_db_1.one_pk_no_uk",
            "test_db_1.no_pk_one_uk",
            "test_db_1.no_pk_multi_uk",
            "test_db_1.one_pk_multi_uk",
        ] {
            tbs.push(tb.to_string());
        }
        tbs
    }

    pub fn get_default_tb_cols() -> Vec<String> {
        let mut cols = Vec::new();
        for col in vec![
            "f_0", "f_1", "f_2", "f_3", "f_4", "f_5", "f_6", "f_7", "f_8", "f_9", "f_10", "f_11",
            "f_12", "f_13", "f_14", "f_15", "f_16", "f_17", "f_18", "f_19", "f_20", "f_21", "f_22",
            "f_23", "f_24", "f_25", "f_26", "f_27", "f_28",
        ] {
            cols.push(col.to_string());
        }
        cols
    }

    pub async fn new(task_config: &str) -> Result<Self, Error> {
        let mut src_conn_pool_mysql = None;
        let mut dst_conn_pool_mysql = None;
        let mut src_conn_pool_pg = None;
        let mut dst_conn_pool_pg = None;
        let (extractor_config, sinker_config, _, _, _) = ConfigLoader::load(task_config)?;

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
        })
    }

    pub async fn run_snapshot_test(
        &self,
        src_ddl_file: &str,
        dst_ddl_file: &str,
        src_dml_file: &str,
        task_config_file: &str,
        src_tbs: &Vec<String>,
        dst_tbs: &Vec<String>,
        cols_list: &Vec<Vec<String>>,
    ) -> Result<(), Error> {
        // prepare src and dst tables
        self.prepare_test_tbs(src_ddl_file, dst_ddl_file).await?;

        // prepare src data
        let src_dml_sqls = TestRunner::load_sqls(src_dml_file)?;
        self.execute_src_sqls(&src_dml_sqls).await?;

        // start task
        TaskRunner::start_task(task_config_file).await?;

        for i in 0..src_tbs.len() {
            let src_tb = src_tbs[i].as_str();
            let dst_tb = dst_tbs[i].as_str();
            let cols = &cols_list[i];
            let res = self.compare_tb_data(src_tb, dst_tb, cols).await?;
            assert!(res);
        }

        Ok(())
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
        for sql in sqls {
            if let Some(pool) = &self.src_conn_pool_mysql {
                let query = sqlx::query(sql);
                query.execute(pool).await.unwrap();
            }
            if let Some(pool) = &self.src_conn_pool_pg {
                let query = sqlx::query(sql);
                query.execute(pool).await.unwrap();
            }
        }
        Ok(())
    }

    pub async fn compare_data_for_tbs(
        &self,
        src_tbs: &Vec<String>,
        dst_tbs: &Vec<String>,
        cols: &Vec<String>,
    ) -> Result<bool, Error> {
        for i in 0..src_tbs.len() {
            let src_tb = &src_tbs[i];
            let dst_tb = &dst_tbs[i];
            if !self.compare_tb_data(src_tb, dst_tb, cols).await? {
                println!(
                    "compare_tb_data failed, src_tb: {}, dst_tb: {}",
                    src_tb, dst_tb
                );
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
        let src_sql = format!("select * from {} order by {}", src_tb, cols[0]);
        let dst_sql = format!("select * from {} order by {}", dst_tb, cols[0]);

        let src_data = if let Some(pool) = &self.src_conn_pool_mysql {
            self.fetch_data_mysql(&src_sql, cols, pool).await?
        } else if let Some(pool) = &self.src_conn_pool_pg {
            self.fetch_data_pg(&src_sql, cols, pool).await?
        } else {
            Vec::new()
        };

        let dst_data = if let Some(pool) = &self.dst_conn_pool_mysql {
            self.fetch_data_mysql(&dst_sql, cols, pool).await?
        } else if let Some(pool) = &self.dst_conn_pool_pg {
            self.fetch_data_pg(&src_sql, cols, pool).await?
        } else {
            Vec::new()
        };

        println!(
            "comparing row data for src_tb: {}, src_data count: {}",
            src_tb,
            src_data.len()
        );
        Ok(Self::compare_row_data(cols, &src_data, &dst_data))
    }

    async fn fetch_data_mysql(
        &self,
        sql: &str,
        cols: &Vec<String>,
        conn_pool: &Pool<MySql>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, Error> {
        let query = sqlx::query(sql);
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
        sql: &str,
        cols: &Vec<String>,
        conn_pool: &Pool<Postgres>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, Error> {
        let query = sqlx::query(sql);
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
