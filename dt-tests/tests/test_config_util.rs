use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::Read,
    path::Path,
};

use configparser::ini::Ini;
use dt_common::config::{
    config_enums::{DbType, ParallelType},
    extractor_config::ExtractorConfig,
    sinker_config::SinkerConfig,
    task_config::TaskConfig,
};

pub struct TestConfigUtil {}

const PIPELINE: &str = "pipeline";
const EXTRACTOR: &str = "extractor";
const SINKER: &str = "sinker";
const RUNTIME: &str = "runtime";
const PARALLEL_TYPE: &str = "parallel_type";
const PARALLEL_SIZE: &str = "parallel_size";
const BATCH_SIZE: &str = "batch_size";
const URL: &str = "url";
const TEST_PROJECT: &str = "dt-tests";

#[allow(dead_code)]
impl TestConfigUtil {
    pub const LOG_DIR: &str = "log_dir";
    pub const EXTRACTOR_CHECK_LOG_DIR: &str = "extractor_check_log_dir";
    pub const SINKER_CHECK_LOG_DIR: &str = "sinker_check_log_dir";

    pub fn get_project_root() -> String {
        project_root::get_project_root()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    pub fn get_absolute_dir(relative_dir: &str) -> String {
        format!(
            "{}/{}/tests/{}",
            project_root::get_project_root().unwrap().to_str().unwrap(),
            TEST_PROJECT,
            relative_dir
        )
    }

    pub fn transfer_config(config: Vec<(&str, &str, &str)>) -> Vec<(String, String, String)> {
        let mut result = Vec::new();
        for i in config.iter() {
            result.push((i.0.to_string(), i.1.to_string(), i.2.to_string()));
        }
        result
    }

    pub fn get_default_configs() -> Vec<Vec<(String, String, String)>> {
        vec![
            Self::get_default_serial_config(),
            Self::get_default_parallel_config(),
            Self::get_default_rdb_merge_config(),
        ]
    }

    pub fn get_default_serial_config() -> Vec<(String, String, String)> {
        Self::transfer_config(vec![
            (
                PIPELINE,
                PARALLEL_TYPE,
                &ParallelType::RdbPartition.to_string(),
            ),
            (PIPELINE, PARALLEL_SIZE, "1"),
            (SINKER, BATCH_SIZE, "1"),
        ])
    }

    pub fn get_default_parallel_config() -> Vec<(String, String, String)> {
        Self::transfer_config(vec![
            (
                PIPELINE,
                PARALLEL_TYPE,
                &ParallelType::RdbPartition.to_string(),
            ),
            (PIPELINE, PARALLEL_SIZE, "2"),
            (SINKER, BATCH_SIZE, "1"),
        ])
    }

    pub fn get_default_rdb_merge_config() -> Vec<(String, String, String)> {
        Self::transfer_config(vec![
            (PIPELINE, PARALLEL_TYPE, &ParallelType::RdbMerge.to_string()),
            (PIPELINE, PARALLEL_SIZE, "2"),
            (SINKER, BATCH_SIZE, "2"),
        ])
    }

    pub fn update_task_config_url(
        src_task_config_file: &str,
        dst_task_config_file: &str,
        project_root: &str,
    ) {
        let env_path = format!("{}/{}/tests/.env", project_root, TEST_PROJECT);
        dotenv::from_path(env_path).unwrap();

        let config = TaskConfig::new(&src_task_config_file);
        let mut update_configs = Vec::new();

        match &config.extractor.get_db_type() {
            DbType::Mysql => {
                update_configs.push((
                    EXTRACTOR.to_string(),
                    URL.to_string(),
                    env::var("mysql_extractor_url").unwrap(),
                ));
            }

            DbType::Pg => {
                update_configs.push((
                    EXTRACTOR.to_string(),
                    URL.to_string(),
                    env::var("pg_extractor_url").unwrap(),
                ));
            }

            DbType::Mongo => {
                update_configs.push((
                    EXTRACTOR.to_string(),
                    URL.to_string(),
                    env::var("mongo_extractor_url").unwrap(),
                ));
            }

            _ => {}
        }

        match &config.sinker.get_db_type() {
            DbType::Mysql => {
                update_configs.push((
                    SINKER.to_string(),
                    URL.to_string(),
                    env::var("mysql_sinker_url").unwrap(),
                ));
            }

            DbType::Pg => {
                update_configs.push((
                    SINKER.to_string(),
                    URL.to_string(),
                    env::var("pg_sinker_url").unwrap(),
                ));
            }

            DbType::Mongo => {
                update_configs.push((
                    SINKER.to_string(),
                    URL.to_string(),
                    env::var("mongo_sinker_url").unwrap(),
                ));
            }

            _ => {}
        }

        TestConfigUtil::update_task_config(
            &src_task_config_file,
            &dst_task_config_file,
            &update_configs,
        );
    }

    pub fn update_task_config_log_dir(
        src_task_config_file: &str,
        dst_task_config_file: &str,
        project_root: &str,
    ) -> HashMap<String, String> {
        let config = TaskConfig::new(&src_task_config_file);
        let mut update_configs = Vec::new();

        // runtime/log4rs_file
        let log4rs_file = format!("{}/{}", project_root, config.runtime.log4rs_file);
        update_configs.push((RUNTIME.to_string(), "log4rs_file".to_string(), log4rs_file));

        // runtime/log_dir
        let log_dir = format!("{}/{}", project_root, config.runtime.log_dir);
        update_configs.push((RUNTIME.to_string(), "log_dir".to_string(), log_dir.clone()));

        // extractor/check_log_dir
        let mut extractor_check_log_dir = String::new();
        match config.extractor {
            ExtractorConfig::MysqlCheck { check_log_dir, .. }
            | ExtractorConfig::PgCheck { check_log_dir, .. } => {
                extractor_check_log_dir = format!("{}/{}", project_root, check_log_dir);
                update_configs.push((
                    EXTRACTOR.to_string(),
                    "check_log_dir".to_string(),
                    extractor_check_log_dir.clone(),
                ));
            }
            _ => {}
        }

        // sinker/check_log_dir
        let mut sinker_check_log_dir = String::new();
        match config.sinker {
            SinkerConfig::MysqlCheck { check_log_dir, .. }
            | SinkerConfig::PgCheck { check_log_dir, .. } => {
                if let Some(dir) = check_log_dir {
                    if !dir.is_empty() {
                        sinker_check_log_dir = format!("{}/{}", project_root, dir);
                        update_configs.push((
                            SINKER.to_string(),
                            "check_log_dir".to_string(),
                            sinker_check_log_dir.clone(),
                        ));
                    }
                }
            }
            _ => {}
        }

        if sinker_check_log_dir.is_empty() {
            sinker_check_log_dir = format!("{}/check", log_dir);
        }

        TestConfigUtil::update_task_config(
            &src_task_config_file,
            &dst_task_config_file,
            &update_configs,
        );

        let mut updated_config_fields = HashMap::new();
        updated_config_fields.insert(Self::LOG_DIR.to_string(), log_dir);
        updated_config_fields.insert(
            Self::EXTRACTOR_CHECK_LOG_DIR.to_string(),
            extractor_check_log_dir,
        );
        updated_config_fields.insert(Self::SINKER_CHECK_LOG_DIR.to_string(), sinker_check_log_dir);
        updated_config_fields
    }

    pub fn update_task_config(
        src_task_config: &str,
        dst_task_config: &str,
        config: &Vec<(String, String, String)>,
    ) {
        let mut config_str = String::new();
        File::open(src_task_config)
            .unwrap()
            .read_to_string(&mut config_str)
            .unwrap();
        let mut ini = Ini::new();
        ini.read(config_str).unwrap();

        for (section, key, value) in config.iter() {
            ini.set(section, key, Some(value.to_string()));
        }

        let path = Path::new(&dst_task_config);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        File::create(&dst_task_config).unwrap().set_len(0).unwrap();
        ini.write(dst_task_config).unwrap();
    }

    pub fn should_do_clean_or_not() -> bool {
        let opt = env::var("do_clean_after_test");
        opt.is_ok_and(|x| x == "true")
    }
}
