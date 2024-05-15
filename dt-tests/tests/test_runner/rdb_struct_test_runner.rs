use std::collections::{HashMap, HashSet};
use dt_connector::meta_fetcher::{
    mysql::mysql_struct_check_fetcher::MysqlStructCheckFetcher,
    pg::pg_struct_check_fetcher::PgStructCheckFetcher,
};

use super::{base_test_runner::BaseTestRunner, rdb_test_runner::RdbTestRunner};

pub struct RdbStructTestRunner {
    pub base: RdbTestRunner,
}

impl RdbStructTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let base = RdbTestRunner::new_default(relative_test_dir).await.unwrap();
        Ok(Self { base })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.base.close().await
    }

    pub async fn run_mysql_struct_test(&mut self) -> anyhow::Result<()> {
        self.base.execute_prepare_sqls().await?;
        self.base.base.start_task().await?;

        let expect_ddl_sqls = self.load_expect_ddl_sqls().await;
        let src_check_fetcher = MysqlStructCheckFetcher {
            conn_pool: self.base.src_conn_pool_mysql.as_mut().unwrap().clone(),
        };
        let dst_check_fetcher = MysqlStructCheckFetcher {
            conn_pool: self.base.dst_conn_pool_mysql.as_mut().unwrap().clone(),
        };

        let get_sql_lines = |sql: &str| -> HashSet<String> {
            let mut line_set = HashSet::new();
            let lines: Vec<&str> = sql.split("\n").collect();
            for line in lines {
                line_set.insert(line.trim_end_matches(",").to_owned());
            }
            line_set
        };

        let (src_db_tbs, dst_db_tbs) = self.base.get_compare_db_tbs().unwrap();
        for i in 0..src_db_tbs.len() {
            let src_ddl_sql = src_check_fetcher
                .fetch_table(&src_db_tbs[i].0, &src_db_tbs[i].1)
                .await;
            let dst_ddl_sql = dst_check_fetcher
                .fetch_table(&dst_db_tbs[i].0, &dst_db_tbs[i].1)
                .await;
            let key = format!("{}.{}", &dst_db_tbs[i].0, &dst_db_tbs[i].1);
            let expect_ddl_sql = expect_ddl_sqls.get(&key).unwrap().to_owned();

            println!("src_ddl_sql: {}\n", src_ddl_sql);
            println!("dst_ddl_sql: {}\n", dst_ddl_sql);
            println!("expect_ddl_sql: {}\n", expect_ddl_sql);
            // show create table may return sqls with indexes in different orders during tests,
            // so here we just compare all lines of the sqls.
            let dst_ddl_sql_lines = get_sql_lines(&dst_ddl_sql);
            let expect_ddl_sql_lines = get_sql_lines(&expect_ddl_sql);

            println!("dst_ddl_sql_lines:");
            for line in dst_ddl_sql_lines.iter() {
                println!("{}", line);
            }
            println!("\nexpect_ddl_sql_lines:");
            for line in expect_ddl_sql_lines.iter() {
                println!("{}", line);
            }

            assert_eq!(dst_ddl_sql_lines, expect_ddl_sql_lines);
        }

        // show create database
        let mut tested_dbs = HashSet::new();
        for i in 0..src_db_tbs.len() {
            if tested_dbs.contains(&src_db_tbs[i].0) {
                continue;
            }

            let src_ddl_sql = src_check_fetcher.fetch_database(&src_db_tbs[i].0).await;
            let dst_ddl_sql = src_check_fetcher.fetch_database(&dst_db_tbs[i].0).await;
            let key = format!("{}", &dst_db_tbs[i].0);
            let expect_ddl_sql = expect_ddl_sqls.get(&key).unwrap().to_owned();

            println!("src_ddl_sql: {}\n", src_ddl_sql);
            println!("dst_ddl_sql: {}\n", dst_ddl_sql);
            println!("expect_ddl_sql: {}\n", expect_ddl_sql);

            assert_eq!(dst_ddl_sql, expect_ddl_sql);
            tested_dbs.insert(&src_db_tbs[i].0);
        }

        Ok(())
    }

    pub async fn run_pg_struct_test(&mut self) -> anyhow::Result<()> {
        self.base.execute_prepare_sqls().await?;
        self.base.base.start_task().await?;

        let src_check_fetcher = PgStructCheckFetcher {
            conn_pool: self.base.src_conn_pool_pg.as_mut().unwrap().clone(),
        };
        let dst_check_fetcher = PgStructCheckFetcher {
            conn_pool: self.base.dst_conn_pool_pg.as_mut().unwrap().clone(),
        };

        let (src_db_tbs, dst_db_tbs) = self.base.get_compare_db_tbs().unwrap();
        for i in 0..src_db_tbs.len() {
            let src_table = src_check_fetcher
                .fetch_table(&src_db_tbs[i].0, &src_db_tbs[i].1)
                .await;
            let dst_table = dst_check_fetcher
                .fetch_table(&dst_db_tbs[i].0, &dst_db_tbs[i].1)
                .await;

            println!(
                "comparing src table: {:?} with dst table: {:?}\n",
                src_db_tbs[i], dst_db_tbs[i]
            );
            println!("src_table: {:?}\n", src_table);
            println!("dst_table: {:?}\n", dst_table);
            assert_eq!(src_table, dst_table);
        }

        println!("summary: dst tables: {:?}", src_db_tbs);
        Ok(())
    }

    pub async fn run_struct_test_without_check(&mut self) -> anyhow::Result<()> {
        self.base.execute_prepare_sqls().await?;
        self.base.base.start_task().await
    }

    async fn load_expect_ddl_sqls(&self) -> HashMap<String, String> {
        let mut ddl_sqls = HashMap::new();

        let version = self.base.get_dst_mysql_version().await;
        let ddl_file = if version.starts_with("5.") {
            format!("{}/expect_ddl_5.7.sql", self.base.base.test_dir)
        } else {
            format!("{}/expect_ddl_8.0.sql", self.base.base.test_dir)
        };
        let lines = BaseTestRunner::load_file(&ddl_file);
        let mut lines = lines.iter().peekable();

        while let Some(line) = lines.next() {
            if line.trim().is_empty() {
                continue;
            }

            let key = line.trim().to_owned();
            let mut sql = String::new();
            while let Some(line) = lines.next() {
                if line.trim().is_empty() {
                    break;
                }
                sql.push_str(line);
                sql.push('\n');
            }
            ddl_sqls.insert(key, sql.trim().to_owned());
        }
        ddl_sqls
    }
}
