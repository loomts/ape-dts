use dt_common::{error::Error, utils::time_util::TimeUtil};
use dt_task::task_runner::TaskRunner;
use std::{
    collections::HashMap,
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
    pub updated_config_fields: HashMap<String, String>,
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
        let updated_config_fields = TestConfigUtil::update_task_config_log_dir(
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

        let (
            src_dml_sqls,
            dst_dml_sqls,
            src_ddl_sqls,
            dst_ddl_sqls,
            src_clean_sqls,
            dst_clean_sqls,
        ) = Self::load_sqls(&test_dir);

        Ok(Self {
            task_config_file: dst_task_config_file,
            test_dir,
            src_dml_sqls,
            dst_dml_sqls,
            src_ddl_sqls,
            dst_ddl_sqls,
            src_clean_sqls,
            dst_clean_sqls,
            updated_config_fields,
        })
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
    ) -> (
        Vec<String>,
        Vec<String>,
        Vec<String>,
        Vec<String>,
        Vec<String>,
        Vec<String>,
    ) {
        let load = |sql_file: &str| -> Vec<String> {
            let full_sql_path = format!("{}/{}", test_dir, sql_file);
            if !Self::check_file_exists(&full_sql_path) {
                return Vec::new();
            }

            let mut lines = Self::load_file(&full_sql_path);
            let mut sqls = Vec::new();
            for sql in lines.drain(..) {
                let sql = sql.trim();
                if sql.is_empty() || sql.starts_with("--") {
                    continue;
                }
                sqls.push(sql.to_string());
            }
            sqls
        };

        (
            load("src_dml.sql"),
            load("dst_dml.sql"),
            load("src_ddl.sql"),
            load("dst_ddl.sql"),
            load("src_clean.sql"),
            load("dst_clean.sql"),
        )
    }

    pub fn check_file_exists(file: &str) -> bool {
        fs::metadata(file).is_ok()
    }
}
