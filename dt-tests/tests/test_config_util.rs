use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
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

    pub const REPLACE_PARAM: &str = "replace";
    pub const OVERRIDE_WHOLE: &str = "override";

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

    // result: (absolute_sub_path, sub_path_dir_name)
    pub fn get_absolute_sub_dir(relative_dir: &str) -> Vec<(String, String)> {
        let mut result_dir: Vec<(String, String)> = vec![];

        let absolute_dir = TestConfigUtil::get_absolute_dir(relative_dir);
        let path = PathBuf::from(absolute_dir.as_str());

        let entries = fs::read_dir(path).unwrap();
        for entry in entries {
            if let Ok(sub_path) = entry {
                if sub_path.path().is_dir() {
                    let sub_path_dir = sub_path.file_name().to_string_lossy().to_string();
                    result_dir.push((format!("{}/{}", absolute_dir, sub_path_dir), sub_path_dir));
                }
            }
        }

        result_dir
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

    pub fn load_env(test_dir: &str, env_file_name: &str) -> bool {
        let mut env_file = String::new();
        // recursively search .env from test dir up to parent dirs
        let mut dir = Path::new(test_dir);
        let final_file_name = if env_file_name == "" {
            ".env"
        } else {
            env_file_name
        };
        loop {
            let path = dir.join(final_file_name);
            if fs::metadata(&path).is_ok() {
                env_file = path.to_str().unwrap().to_string();
                break;
            }

            if let Some(parent_dir) = dir.parent() {
                dir = parent_dir;
            } else {
                break;
            }
        }

        println!("use env file: {}", env_file);
        if env_file.is_empty() {
            return false;
        }

        dotenv::from_path(env_file).unwrap();

        true
    }

    pub fn update_task_config_url(
        src_task_config_file: &str,
        dst_task_config_file: &str,
        test_dir: &str,
        policy: &str,
    ) {
        // environment variable settings in .env.local have higher priority
        Self::load_env(test_dir, ".env.local");

        if !Self::load_env(test_dir, ".env") {
            return;
        }

        let config = TaskConfig::new(&src_task_config_file);
        let mut update_configs = Vec::new();

        match policy {
            Self::REPLACE_PARAM => {
                update_configs.push((
                    EXTRACTOR.to_string(),
                    URL.to_string(),
                    Self::convert_with_envs(config.extractor_basic.url).unwrap(),
                ));
                update_configs.push((
                    SINKER.to_string(),
                    URL.to_string(),
                    Self::convert_with_envs(config.sinker_basic.url).unwrap(),
                ));
            }
            _ => {
                match env::var("override_enable") {
                    Ok(v) => {
                        if v.eq("false") {
                            return;
                        }
                    }
                    Err(_) => {}
                }

                let extractor_url = match &config.extractor_basic.db_type {
                    DbType::Mysql => env::var("mysql_extractor_url").unwrap(),
                    DbType::Pg => env::var("pg_extractor_url").unwrap(),
                    DbType::Mongo => env::var("mongo_extractor_url").unwrap(),
                    DbType::Redis => env::var("redis_extractor_url").unwrap(),
                    DbType::Kafka => env::var("kafka_extractor_url").unwrap(),
                    _ => String::new(),
                };
                update_configs.push((EXTRACTOR.into(), URL.into(), extractor_url));

                let sinker_url = match &config.sinker_basic.db_type {
                    DbType::Mysql => env::var("mysql_sinker_url").unwrap(),
                    DbType::Pg => env::var("pg_sinker_url").unwrap(),
                    DbType::Mongo => env::var("mongo_sinker_url").unwrap(),
                    DbType::Redis => env::var("redis_sinker_url").unwrap(),
                    DbType::Kafka => env::var("kafka_sinker_url").unwrap(),
                    _ => String::new(),
                };
                update_configs.push((SINKER.into(), URL.into(), sinker_url));
            }
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

    // convert_with_envs: format the database_url with envs, such as:
    // change: mysql://{test_user}:{test_password}@{test_url}
    // to: mysql://test:123456@127.0.0.1:3306
    // when have the envs: test_user=test, test_password=123456, test_url=127.0.0.1:3306
    pub fn convert_with_envs(database_url: String) -> Option<String> {
        if database_url.is_empty() {
            return None;
        }
        let (mut new_url_bytes, mut pos, mut left_pos): (Vec<u8>, i64, i64) = (vec![], 0, -1);

        for ch in database_url.chars() {
            if ch == '{' {
                left_pos = pos;
            } else if ch == '}' && pos > left_pos && left_pos >= 0 {
                let new_env = String::from_utf8(
                    database_url.as_bytes()[(left_pos + 1) as usize..pos as usize].to_vec(),
                )
                .unwrap();
                if env::var(&new_env).is_ok() {
                    let env_val_tmp = env::var(new_env).unwrap();
                    new_url_bytes.extend_from_slice(env_val_tmp.as_bytes());
                }
                left_pos = -1;
            } else if left_pos == -1 {
                new_url_bytes.push(ch as u8);
            }
            pos += 1;
        }

        Some(String::from_utf8(new_url_bytes).unwrap())
    }
}
