use std::{fs::File, io::Read};

use configparser::ini::Ini;

use crate::error::Error;

use super::{
    extractor_config::ExtractorConfig, filter_config::FilterConfig, router_config::RouterConfig,
    runtime_config::RuntimeConfig, sinker_config::SinkerConfig,
};

pub struct ConfigLoader {}

impl ConfigLoader {
    pub fn load(
        task_config: &str,
    ) -> Result<
        (
            ExtractorConfig,
            SinkerConfig,
            RuntimeConfig,
            FilterConfig,
            RouterConfig,
        ),
        Error,
    > {
        let mut config_str = String::new();
        File::open(task_config)?.read_to_string(&mut config_str)?;
        let mut ini = Ini::new();
        ini.read(config_str).unwrap();

        Ok((
            Self::load_extractor_config(&ini)?,
            Self::load_sinker_config(&ini)?,
            Self::load_runtime_config(&ini),
            Self::load_filter_config(&ini)?,
            Self::load_router_config(&ini)?,
        ))
    }

    fn load_extractor_config(ini: &Ini) -> Result<ExtractorConfig, Error> {
        let extractor_type = ini.get("extractor", "type").unwrap();
        match extractor_type.as_str() {
            "mysql_snapshot" => Ok(ExtractorConfig::MysqlSnapshot {
                url: ini.get("extractor", "url").unwrap(),
                do_tb: "".to_string(),
            }),

            "mysql_cdc" => Ok(ExtractorConfig::MysqlCdc {
                url: ini.get("extractor", "url").unwrap(),
                binlog_filename: ini.get("extractor", "binlog_filename").unwrap(),
                binlog_position: ini
                    .getuint("extractor", "binlog_position")
                    .unwrap()
                    .unwrap() as u32,
                server_id: ini.getuint("extractor", "server_id").unwrap().unwrap() as u64,
            }),

            "pg_snapshot" => Ok(ExtractorConfig::PgSnapshot {
                url: ini.get("extractor", "url").unwrap(),
                do_tb: "".to_string(),
            }),

            "pg_cdc" => Ok(ExtractorConfig::PgCdc {
                url: ini.get("extractor", "url").unwrap(),
                slot_name: ini.get("extractor", "slot_name").unwrap(),
                start_lsn: ini.get("extractor", "start_lsn").unwrap(),
            }),

            _ => Err(Error::Unexpected {
                error: "unexpected extractor type".to_string(),
            }),
        }
    }

    fn load_sinker_config(ini: &Ini) -> Result<SinkerConfig, Error> {
        let sinker_type = ini.get("sinker", "type").unwrap();
        match sinker_type.as_str() {
            "mysql" => Ok(SinkerConfig::Mysql {
                url: ini.get("sinker", "url").unwrap(),
            }),

            "pg" => Ok(SinkerConfig::Pg {
                url: ini.get("sinker", "url").unwrap(),
            }),

            _ => Err(Error::Unexpected {
                error: "unexpected sinker type".to_string(),
            }),
        }
    }

    fn load_runtime_config(ini: &Ini) -> RuntimeConfig {
        RuntimeConfig {
            buffer_size: ini.getuint("runtime", "buffer_size").unwrap().unwrap() as usize,
            parallel_size: ini.getuint("runtime", "parallel_size").unwrap().unwrap() as usize,
            batch_size: ini.getuint("runtime", "batch_size").unwrap().unwrap() as usize,
            log_level: ini.get("runtime", "log_level").unwrap(),
            log_dir: ini.get("runtime", "log_dir").unwrap(),
        }
    }

    fn load_filter_config(ini: &Ini) -> Result<FilterConfig, Error> {
        let extractor_type = ini.get("extractor", "type").unwrap();
        match extractor_type.as_str() {
            "mysql_snapshot" | "mysql_cdc" | "pg_snapshot" | "pg_cdc" => Ok(FilterConfig::Rdb {
                do_dbs: ini.get("filter", "do_dbs").unwrap(),
                ignore_dbs: ini.get("filter", "ignore_dbs").unwrap(),
                do_tbs: ini.get("filter", "do_tbs").unwrap(),
                ignore_tbs: ini.get("filter", "ignore_tbs").unwrap(),
                do_events: ini.get("filter", "do_events").unwrap(),
            }),

            _ => Err(Error::Unexpected {
                error: "unexpected extractor type".to_string(),
            }),
        }
    }

    fn load_router_config(ini: &Ini) -> Result<RouterConfig, Error> {
        let extractor_type = ini.get("sinker", "type").unwrap();
        match extractor_type.as_str() {
            "mysql" | "pg" => Ok(RouterConfig::Rdb {
                db_map: ini.get("router", "db_map").unwrap(),
                tb_map: ini.get("router", "tb_map").unwrap(),
                field_map: ini.get("router", "field_map").unwrap(),
            }),

            _ => Err(Error::Unexpected {
                error: "unexpected sinker type".to_string(),
            }),
        }
    }
}
