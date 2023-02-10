use std::{env, fs::File, io::Read, path::PathBuf};

use futures::TryStreamExt;
use sqlx::{MySql, Pool, Row};

use crate::{config::env_var::EnvVar, error::Error, task::task_util::TaskUtil};

pub struct TestRunner {
    pub env_var: EnvVar,
    pub src_conn_pool: Pool<MySql>,
    pub dst_conn_pool: Pool<MySql>,
}

#[allow(dead_code)]
impl TestRunner {
    pub async fn new(env_file: &str) -> Result<Self, Error> {
        let env_path = env::current_dir().unwrap().join(env_file);
        dotenv::from_path(env_path).unwrap();
        let env_var = EnvVar::new().unwrap();

        let src_url = env::var("SRC_URL").unwrap();
        let dst_url = env::var("DST_URL").unwrap();

        let src_conn_pool = TaskUtil::create_mysql_conn_pool(&src_url, 1, true).await?;
        let dst_conn_pool = TaskUtil::create_mysql_conn_pool(&dst_url, 1, true).await?;

        Ok(Self {
            env_var,
            src_conn_pool,
            dst_conn_pool,
        })
    }

    pub async fn prepare_test_tbs(
        &self,
        src_ddl_file: &str,
        dst_ddl_file: &str,
    ) -> Result<(), Error> {
        let src_ddl_sqls = self.load_sqls(src_ddl_file).await?;
        let dst_ddl_sqls = self.load_sqls(dst_ddl_file).await?;
        self.execute_sqls(&src_ddl_sqls, &self.src_conn_pool)
            .await?;
        self.execute_sqls(&dst_ddl_sqls, &self.dst_conn_pool)
            .await?;
        Ok(())
    }

    pub async fn load_task_config(&self, config_file: &str) -> Result<String, Error> {
        let mut config_str = String::new();
        let config_path = env::current_dir().unwrap().join(config_file);
        File::open(PathBuf::from(config_path))?.read_to_string(&mut config_str)?;
        Ok(config_str)
    }

    pub async fn load_sqls(&self, sql_file: &str) -> Result<Vec<String>, Error> {
        let mut content = String::new();
        let file_path = env::current_dir().unwrap().join(sql_file);
        File::open(file_path)?.read_to_string(&mut content)?;

        let mut sqls = Vec::new();
        for sql in content.split("\n") {
            if !sql.is_empty() {
                sqls.push(sql.to_string());
            }
        }
        Ok(sqls)
    }

    pub async fn execute_sqls(
        &self,
        sqls: &Vec<String>,
        conn_pool: &Pool<MySql>,
    ) -> Result<(), Error> {
        for sql in sqls {
            let query = sqlx::query(sql);
            query.execute(conn_pool).await?;
        }
        Ok(())
    }

    pub async fn fetch_data(
        &self,
        sql: &str,
        cols: &Vec<&str>,
        conn_pool: &Pool<MySql>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, Error> {
        let query = sqlx::query(sql);
        let mut rows = query.fetch(conn_pool);

        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let mut row_values = Vec::new();
            for col in cols {
                let value: Option<Vec<u8>> = row.get_unchecked(col);
                row_values.push(value);
            }
            result.push(row_values);
        }

        Ok(result)
    }

    pub async fn compare_data_for_tbs(
        &self,
        src_tbs: &Vec<&str>,
        dst_tbs: &Vec<&str>,
        cols: &Vec<&str>,
    ) -> Result<bool, Error> {
        for i in 0..src_tbs.len() {
            let src_tb = src_tbs[i];
            let dst_tb = dst_tbs[i];
            if !self.compare_tb_data(src_tb, dst_tb, &cols).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub async fn compare_tb_data(
        &self,
        src_tb: &str,
        dst_tb: &str,
        cols: &Vec<&str>,
    ) -> Result<bool, Error> {
        let src_sql = format!("select * from {}", src_tb);
        let dst_sql = format!("select * from {}", dst_tb);
        let src_data = self.fetch_data(&src_sql, cols, &self.src_conn_pool).await?;
        let dst_data = self.fetch_data(&dst_sql, cols, &self.dst_conn_pool).await?;
        Ok(self.compare_row_data(&src_data, &dst_data))
    }

    pub fn compare_row_data(
        &self,
        src_data: &Vec<Vec<Option<Vec<u8>>>>,
        dst_data: &Vec<Vec<Option<Vec<u8>>>>,
    ) -> bool {
        for i in 0..src_data.len() {
            let src_row_data = &src_data[i];
            let dst_row_data = &dst_data[i];

            for j in 0..src_row_data.len() {
                let src_col_value = &src_row_data[j];
                let dst_col_value = &dst_row_data[j];
                if src_col_value != dst_col_value {
                    return false;
                }
            }
        }
        true
    }
}
