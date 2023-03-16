use std::{fs::File, io::Read};

use configparser::ini::Ini;

pub struct TestConfigUtil {}

#[allow(dead_code)]
impl TestConfigUtil {
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
            ("pipeline", "type", "parralel"),
            ("pipeline", "parallel_size", "1"),
            ("sinker", "batch_size", "1"),
        ])
    }

    pub fn get_default_parallel_config() -> Vec<(String, String, String)> {
        Self::transfer_config(vec![
            ("pipeline", "type", "parralel"),
            ("pipeline", "parallel_size", "2"),
            ("sinker", "batch_size", "1"),
        ])
    }

    pub fn get_default_rdb_merge_config() -> Vec<(String, String, String)> {
        Self::transfer_config(vec![
            ("pipeline", "type", "rdb_merge"),
            ("pipeline", "parallel_size", "2"),
            ("sinker", "batch_size", "2"),
        ])
    }

    pub fn update_task_config(task_config_file: &str, config: &Vec<(String, String, String)>) {
        let mut config_str = String::new();
        File::open(task_config_file)
            .unwrap()
            .read_to_string(&mut config_str)
            .unwrap();
        let mut ini = Ini::new();
        ini.read(config_str).unwrap();

        for (section, key, value) in config.iter() {
            ini.set(section, key, Some(value.to_string()));
        }
        ini.write(task_config_file).unwrap();
    }
}
