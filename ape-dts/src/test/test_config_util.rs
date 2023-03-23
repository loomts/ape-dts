use std::{
    fs::{self, File},
    io::Read,
    path::Path,
};

use configparser::ini::Ini;
use dt_common::config::config_enums::ParallelType;
use strum::AsStaticRef;

pub struct TestConfigUtil {}

const PIPELINE: &str = "pipeline";
const SINKER: &str = "sinker";
const PARALLEL_TYPE: &str = "parallel_type";
const PARALLEL_SIZE: &str = "parallel_size";
const BATCH_SIZE: &str = "batch_size";

#[allow(dead_code)]
impl TestConfigUtil {
    pub fn get_project_root() -> String {
        project_root::get_project_root()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    pub fn get_absolute_dir(relative_dir: &str) -> String {
        format!(
            "{}/ape-dts/src/test/{}",
            project_root::get_project_root().unwrap().to_str().unwrap(),
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
                ParallelType::RdbPartition.as_static(),
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
                ParallelType::RdbPartition.as_static(),
            ),
            (PIPELINE, PARALLEL_SIZE, "2"),
            (SINKER, BATCH_SIZE, "1"),
        ])
    }

    pub fn get_default_rdb_merge_config() -> Vec<(String, String, String)> {
        Self::transfer_config(vec![
            (PIPELINE, PARALLEL_TYPE, ParallelType::RdbMerge.as_static()),
            (PIPELINE, PARALLEL_SIZE, "2"),
            (SINKER, BATCH_SIZE, "2"),
        ])
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
}
