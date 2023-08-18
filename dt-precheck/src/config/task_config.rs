use std::{fs::File, io::Read};

use configparser::ini::Ini;
use dt_common::error::Error;

use super::precheck_config::PrecheckConfig;

const PRECHECK: &str = "precheck";

pub struct PrecheckTaskConfig {
    pub precheck: PrecheckConfig,
}

impl PrecheckTaskConfig {
    pub fn new(task_config_file: &str) -> Result<Self, Error> {
        let mut config_str = String::new();
        File::open(task_config_file)
            .unwrap()
            .read_to_string(&mut config_str)
            .unwrap();
        let mut ini = Ini::new();
        ini.read(config_str).unwrap();

        let precheck_config = Self::load_precheck_config(&ini)?;
        Ok(Self {
            precheck: precheck_config,
        })
    }

    fn load_precheck_config(ini: &Ini) -> Result<PrecheckConfig, Error> {
        let (do_struct_opt, do_cdc_opt): (Option<String>, Option<String>) = (
            ini.get(PRECHECK, "do_struct_init"),
            ini.get(PRECHECK, "do_cdc"),
        );
        if let (Some(do_struct), Some(do_cdc)) = (do_struct_opt, do_cdc_opt) {
            Ok(PrecheckConfig {
                do_struct_init: do_struct.parse().unwrap(),
                do_cdc: do_cdc.parse().unwrap(),
            })
        } else {
            Err(Error::ConfigError(
                "config is not valid for precheck.".into(),
            ))
        }
    }
}
