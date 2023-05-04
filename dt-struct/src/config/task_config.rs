use std::{fs::File, io::Read};

use configparser::ini::Ini;

use crate::error::Error;

use super::struct_config::StructConfig;

const STRUCT: &str = "struct";

pub struct StructTaskConfig {
    pub struct_config: StructConfig,
}

impl StructTaskConfig {
    pub fn new(task_config_file: &str) -> Self {
        let mut config_str = String::new();
        File::open(task_config_file)
            .unwrap()
            .read_to_string(&mut config_str)
            .unwrap();
        let mut ini = Ini::new();
        ini.read(config_str).unwrap();

        Self {
            struct_config: Self::load_struct_config(&ini).unwrap(),
        }
    }

    fn load_struct_config(ini: &Ini) -> Result<StructConfig, Error> {
        let mut is_do_check = false;
        match ini.getbool(STRUCT, "is_do_check") {
            Ok(val_opt) => match val_opt {
                Some(val) => is_do_check = val,
                None => {}
            },
            Err(_) => {}
        }
        Ok(StructConfig {
            conflict_policy: ini.get(STRUCT, "conflict_policy").unwrap().parse().unwrap(),
            is_do_check,
        })
    }
}
