use dt_common::{config::task_config::TaskConfig, utils::time_util::TimeUtil};
use dt_connector::data_marker::DataMarker;
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
    pub src_test_sqls: Vec<String>,
    pub dst_test_sqls: Vec<String>,
    pub src_prepare_sqls: Vec<String>,
    pub dst_prepare_sqls: Vec<String>,
    pub src_clean_sqls: Vec<String>,
    pub dst_clean_sqls: Vec<String>,
    pub meta_center_prepare_sqls: Vec<String>,
}

static mut LOG4RS_INITED: bool = false;

#[allow(dead_code)]
impl BaseTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let project_root = TestConfigUtil::get_project_root();
        let tmp_dir = format!("{}/tmp/{}", project_root, relative_test_dir);
        let test_dir = TestConfigUtil::get_absolute_path(relative_test_dir);
        let src_task_config_file = format!("{}/task_config.ini", test_dir);
        let dst_task_config_file = format!("{}/task_config.ini", tmp_dir);

        // update relative path to absolute path in task_config.ini
        TestConfigUtil::update_file_paths_in_task_config(
            &src_task_config_file,
            &dst_task_config_file,
            &project_root,
        );

        // update extractor / sinker urls from .env
        TestConfigUtil::update_task_config_from_env(&dst_task_config_file, &dst_task_config_file);

        let (
            src_test_sqls,
            dst_test_sqls,
            src_prepare_sqls,
            dst_prepare_sqls,
            src_clean_sqls,
            dst_clean_sqls,
            meta_center_prepare_sqls,
        ) = Self::load_sqls(&test_dir);

        Ok(Self {
            task_config_file: dst_task_config_file,
            test_dir,
            src_test_sqls,
            dst_test_sqls,
            src_prepare_sqls,
            dst_prepare_sqls,
            src_clean_sqls,
            dst_clean_sqls,
            meta_center_prepare_sqls,
        })
    }

    pub fn get_config(&self) -> TaskConfig {
        TaskConfig::new(&self.task_config_file).unwrap()
    }

    pub async fn start_task(&self) -> anyhow::Result<()> {
        let enable_log4rs = Self::get_enable_log4rs();
        TaskRunner::new(&self.task_config_file)?
            .start_task(enable_log4rs)
            .await
    }

    pub async fn spawn_task(&self) -> anyhow::Result<JoinHandle<()>> {
        let enable_log4rs = Self::get_enable_log4rs();
        let task_runner = TaskRunner::new(&self.task_config_file)?;
        let task =
            tokio::spawn(async move { task_runner.start_task(enable_log4rs).await.unwrap() });
        Ok(task)
    }

    pub async fn abort_task(&self, task: &JoinHandle<()>) -> anyhow::Result<()> {
        task.abort();
        while !task.is_finished() {
            TimeUtil::sleep_millis(1).await;
        }
        Ok(())
    }

    pub async fn wait_task_finish(&self, task: &JoinHandle<()>) -> anyhow::Result<()> {
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
        Vec<String>,
    ) {
        let load = |sql_file: &str| -> Vec<String> {
            let full_sql_path = format!("{}/{}", test_dir, sql_file);
            if !Self::check_path_exists(&full_sql_path) {
                return Vec::new();
            }
            Self::load_sql_file(&full_sql_path)
        };

        (
            load("src_test.sql"),
            load("dst_test.sql"),
            load("src_prepare.sql"),
            load("dst_prepare.sql"),
            load("src_clean.sql"),
            load("dst_clean.sql"),
            load("meta_center_prepare.sql"),
        )
    }

    fn load_sql_file(sql_file: &str) -> Vec<String> {
        /* sqls content E.g. :
        -- this is comment

        select * from db_1.tb_1;  -- a sql in single line

        ```
        -- a sql in multiple lines
        select *
        from
        db_1.tb_1;
        ```
        */
        let mut sqls = Vec::new();
        let mut in_block = false;
        let mut multi_line_sql = String::new();

        for line in Self::load_file(&sql_file).iter() {
            let line = line.trim();
            if line.is_empty() || line.starts_with("--") {
                continue;
            }

            if line.starts_with("```") {
                if in_block {
                    // block end
                    in_block = false;
                    if !multi_line_sql.is_empty() {
                        // pop the last '\n'
                        multi_line_sql.pop();
                        sqls.push(multi_line_sql.clone());
                    }
                } else {
                    // block start
                    in_block = true;
                    multi_line_sql.clear();
                }
                continue;
            }

            if in_block {
                multi_line_sql.push_str(&line);
                multi_line_sql.push('\n');
            } else {
                sqls.push(line.into());
            }
        }
        sqls
    }

    pub fn check_path_exists(file: &str) -> bool {
        fs::metadata(file).is_ok()
    }

    pub fn get_data_marker(&self) -> Option<DataMarker> {
        let config = self.get_config();
        if let Some(data_marker_config) = config.data_marker {
            let data_marker =
                DataMarker::from_config(&data_marker_config, &config.extractor_basic.db_type)
                    .unwrap();
            return Some(data_marker);
        }
        None
    }
}
