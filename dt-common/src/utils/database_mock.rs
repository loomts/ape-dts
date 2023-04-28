use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
    time::Duration,
};

use sqlx::{mysql::MySqlPoolOptions, postgres::PgPoolOptions, Error, MySql, Pool, Postgres};

use crate::meta::db_enums::DbType;

use super::work_dir_util::WorkDirUtil;

// DatabaseMockUtils: Mock Database struct& data for test.
pub struct DatabaseMockUtils {
    pub pg_pool: Option<Pool<Postgres>>,
    pub mysql_pool: Option<Pool<MySql>>,
    pub db_type: DbType,
}

impl DatabaseMockUtils {
    pub async fn new(
        connection_url: String,
        db_type: DbType,
        pool_size: u32,
        connection_timeout_sec: u64,
    ) -> Result<Self, Error> {
        match db_type {
            DbType::Mysql => {
                let db_pool_result = MySqlPoolOptions::new()
                    .max_connections(pool_size)
                    .acquire_timeout(Duration::from_secs(connection_timeout_sec))
                    .connect(connection_url.as_str())
                    .await;
                match db_pool_result {
                    Ok(pool) => {
                        return Ok(Self {
                            mysql_pool: Option::Some(pool),
                            pg_pool: None,
                            db_type,
                        })
                    }
                    Err(error) => {
                        return Err(error);
                    }
                }
            }
            DbType::Pg => {
                let db_pool_result = PgPoolOptions::new()
                    .max_connections(pool_size)
                    .acquire_timeout(Duration::from_secs(connection_timeout_sec))
                    .connect(connection_url.as_str())
                    .await;
                match db_pool_result {
                    Ok(pool) => {
                        return Ok(Self {
                            mysql_pool: None,
                            pg_pool: Option::Some(pool),
                            db_type,
                        })
                    }
                    Err(error) => {
                        return Err(error);
                    }
                }
            }
        }
    }

    pub async fn load_data(&self, relative_data_path: &str) -> Result<(), Error> {
        match self.db_type {
            DbType::Mysql => {
                if self.mysql_pool.is_none() {
                    println!("mysql_pool is empty.");
                    return Ok(());
                }
            }
            DbType::Pg => {
                if self.pg_pool.is_none() {
                    println!("pg_pool is empty.");
                    return Ok(());
                }
            }
        }

        let absolute_path = WorkDirUtil::get_absolute_by_relative(relative_data_path).unwrap();
        let file_path = Path::new(&absolute_path);
        if !file_path.exists() || !file_path.is_file() {
            println!("path:{} is empty.", absolute_path);
            return Ok(());
        }
        let file = File::open(absolute_path).unwrap();
        for line in BufReader::new(file).lines() {
            if let Ok(sql) = line {
                if sql.is_empty() || sql.starts_with("--") {
                    continue;
                }
                match self.db_type {
                    DbType::Mysql => {
                        let pool: &Pool<MySql>;
                        match &self.mysql_pool {
                            Some(p) => pool = p,
                            _ => return Ok(()),
                        }
                        _ = sqlx::query(&sql).execute(pool).await;
                    }
                    DbType::Pg => {
                        let pool: &Pool<Postgres>;
                        match &self.pg_pool {
                            Some(p) => pool = p,
                            _ => return Ok(()),
                        }
                        _ = sqlx::query(&sql).execute(pool).await;
                    }
                }
                println!("executed sql: {}", sql);
            }
        }
        Ok(())
    }

    pub async fn release_pool(self) {
        match self.mysql_pool {
            Some(p) => p.close().await,
            None => (),
        }
        match self.pg_pool {
            Some(p) => p.close().await,
            None => (),
        }
    }
}
