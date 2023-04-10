use std::{fs::File, io::Read};

use configparser::ini::Ini;

use crate::error::Error;

use super::precheck_config::PrecheckConfig;

const PRECHECK: &str = "precheck";

pub struct PrecheckTaskConfig {
    pub precheck: PrecheckConfig,
}

impl PrecheckTaskConfig {
    pub fn new(task_config_file: &str) -> Self {
        let mut config_str = String::new();
        File::open(task_config_file)
            .unwrap()
            .read_to_string(&mut config_str)
            .unwrap();
        let mut ini = Ini::new();
        ini.read(config_str).unwrap();

        Self {
            precheck: Self::load_precheck_config(&ini).unwrap(),
        }
    }

    fn load_precheck_config(ini: &Ini) -> Result<PrecheckConfig, Error> {
        Ok(PrecheckConfig {
            do_struct_init: ini
                .get(PRECHECK, "do_struct_init")
                .unwrap()
                .parse()
                .unwrap(),
            do_cdc: ini.get(PRECHECK, "do_cdc").unwrap().parse().unwrap(),
        })
    }
}
