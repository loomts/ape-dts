use dt_common::{
    config::{config_enums::DbType, task_config::TaskConfig},
    error::Error,
    utils::time_util::TimeUtil,
};
use dt_task::task_runner::TaskRunner;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
};
use tokio::task::JoinHandle;

use crate::test_config_util::TestConfigUtil;

pub struct BaseTestRunner {
    pub test_dir: String,
    pub task_config_file: String,
    pub src_dml_sqls: Vec<String>,
    pub dst_dml_sqls: Vec<String>,
    pub src_ddl_sqls: Vec<String>,
    pub dst_ddl_sqls: Vec<String>,
    pub src_clean_sqls: Vec<String>,
    pub dst_clean_sqls: Vec<String>,
}

static mut LOG4RS_INITED: bool = false;

#[allow(dead_code)]
impl BaseTestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        Self::new_internal(relative_test_dir, "").await
    }

    pub async fn new_internal(
        relative_test_dir: &str,
        config_tmp_relative_dir: &str,
    ) -> Result<Self, Error> {
        let project_root = TestConfigUtil::get_project_root();
        let tmp_dir = if config_tmp_relative_dir.is_empty() {
            format!("{}/tmp/{}", project_root, relative_test_dir)
        } else {
            format!("{}/tmp/{}", project_root, config_tmp_relative_dir)
        };
        let test_dir = TestConfigUtil::get_absolute_dir(relative_test_dir);
        let src_task_config_file = format!("{}/task_config.ini", test_dir);
        let dst_task_config_file = format!("{}/task_config.ini", tmp_dir);

        // update relative path to absolute path in task_config.ini
        TestConfigUtil::update_task_config_log_dir(
            &src_task_config_file,
            &dst_task_config_file,
            &project_root,
        );

        // update extractor / sinker urls from .env
        TestConfigUtil::update_task_config_from_env(
            &dst_task_config_file,
            &dst_task_config_file,
            &test_dir,
        );

        let config = TaskConfig::new(&dst_task_config_file);

        let (
            src_dml_sqls,
            dst_dml_sqls,
            src_ddl_sqls,
            dst_ddl_sqls,
            src_clean_sqls,
            dst_clean_sqls,
        ) = Self::load_sqls(
            &test_dir,
            &config.extractor_basic.db_type,
            &config.sinker_basic.db_type,
        );

        Ok(Self {
            task_config_file: dst_task_config_file,
            test_dir,
            src_dml_sqls,
            dst_dml_sqls,
            src_ddl_sqls,
            dst_ddl_sqls,
            src_clean_sqls,
            dst_clean_sqls,
        })
    }

    pub fn get_config(&self) -> TaskConfig {
        TaskConfig::new(&self.task_config_file)
    }

    pub async fn start_task(&self) -> Result<(), Error> {
        let enable_log4rs = Self::get_enable_log4rs();
        let task_config = self.task_config_file.clone();
        TaskRunner::new(task_config)
            .await
            .start_task(enable_log4rs)
            .await
    }

    pub async fn spawn_task(&self) -> Result<JoinHandle<()>, Error> {
        let enable_log4rs = Self::get_enable_log4rs();
        let task_runner = TaskRunner::new(self.task_config_file.clone()).await;
        let task =
            tokio::spawn(async move { task_runner.start_task(enable_log4rs).await.unwrap() });
        Ok(task)
    }

    pub async fn wait_task_finish(&self, task: &JoinHandle<()>) -> Result<(), Error> {
        task.abort();
        while !task.is_finished() {
            TimeUtil::sleep_millis(1).await;
        }
        Ok(())
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

    pub fn load_file(file_path: &str) -> Vec<String> {
        if fs::metadata(&file_path).is_err() {
            return Vec::new();
        }

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

    fn load_sqls(
        test_dir: &str,
        src_db_type: &DbType,
        dst_db_type: &DbType,
    ) -> (
        Vec<String>,
        Vec<String>,
        Vec<String>,
        Vec<String>,
        Vec<String>,
        Vec<String>,
    ) {
        let load = |sql_file: &str, db_type: &DbType| -> Vec<String> {
            let full_sql_path = format!("{}/{}", test_dir, sql_file);
            if !Self::check_path_exists(&full_sql_path) {
                return Vec::new();
            }

            if db_type == &DbType::Mysql || db_type == &DbType::Pg {
                Self::load_rdb_sqls(&full_sql_path)
            } else {
                Self::load_non_rdb_sqls(&full_sql_path)
            }
        };

        (
            load("src_dml.sql", src_db_type),
            load("dst_dml.sql", dst_db_type),
            load("src_ddl.sql", src_db_type),
            load("dst_ddl.sql", dst_db_type),
            load("src_clean.sql", src_db_type),
            load("dst_clean.sql", dst_db_type),
        )
    }

    fn load_non_rdb_sqls(sql_file: &str) -> Vec<String> {
        let mut sqls = Vec::new();
        let mut lines = Self::load_file(&sql_file);
        for line in lines.drain(..) {
            let line = line.trim();
            if line.is_empty() || line.starts_with("--") {
                continue;
            }
            sqls.push(line.to_string());
        }
        sqls
    }

    fn load_rdb_sqls(sql_file: &str) -> Vec<String> {
        let mut sqls = Vec::new();
        let sql_start_keywords = vec![
            "create ",
            "drop ",
            "alter ",
            "insert ",
            "update ",
            "delete ",
            "comment ",
            "truncate ",
            "select ",
        ];
        let mut lines = Self::load_file(&sql_file);
        for line in lines.drain(..) {
            let check_line = line.trim().to_lowercase();
            if check_line.is_empty() || check_line.starts_with("--") {
                continue;
            }

            if sql_start_keywords
                .iter()
                .any(|keyword| -> bool { check_line.starts_with(keyword) })
            {
                sqls.push(line);
            } else {
                if let Some(last_sql) = sqls.last_mut() {
                    last_sql.push_str("\n");
                    last_sql.push_str(&line);
                }
            }
        }
        sqls
    }

    pub fn check_path_exists(file: &str) -> bool {
        fs::metadata(file).is_ok()
    }
}
