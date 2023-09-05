use std::{collections::HashMap, fmt::Debug, fs::File, io::Read, str::FromStr};

use configparser::ini::Ini;

use crate::error::Error;

use super::{
    config_enums::{ConflictPolicyEnum, DbType, ExtractType, ParallelType, SinkType},
    extractor_config::ExtractorConfig,
    filter_config::FilterConfig,
    pipeline_config::PipelineConfig,
    resumer_config::ResumerConfig,
    router_config::RouterConfig,
    runtime_config::RuntimeConfig,
    sinker_config::SinkerConfig,
};

#[derive(Clone)]
pub struct TaskConfig {
    pub extractor: ExtractorConfig,
    pub sinker: SinkerConfig,
    pub runtime: RuntimeConfig,
    pub pipeline: PipelineConfig,
    pub filter: FilterConfig,
    pub router: RouterConfig,
    pub resumer: ResumerConfig,
}

const EXTRACTOR: &str = "extractor";
const CHECK_LOG_DIR: &str = "check_log_dir";
const SINKER: &str = "sinker";
const DB_TYPE: &str = "db_type";
const URL: &str = "url";
const PIPELINE: &str = "pipeline";
const RUNTIME: &str = "runtime";
const FILTER: &str = "filter";
const ROUTER: &str = "router";
const RESUMER: &str = "resumer";
const BATCH_SIZE: &str = "batch_size";

impl TaskConfig {
    pub fn new(task_config_file: &str) -> Self {
        let mut config_str = String::new();
        File::open(task_config_file)
            .unwrap()
            .read_to_string(&mut config_str)
            .unwrap();
        let mut ini = Ini::new();
        ini.read(config_str).unwrap();

        Self {
            extractor: Self::load_extractor_config(&ini).unwrap(),
            pipeline: Self::load_pipeline_config(&ini),
            sinker: Self::load_sinker_config(&ini).unwrap(),
            runtime: Self::load_runtime_config(&ini).unwrap(),
            filter: Self::load_filter_config(&ini).unwrap(),
            router: Self::load_router_config(&ini).unwrap(),
            resumer: Self::load_resumer_config(&ini).unwrap(),
        }
    }

    fn load_extractor_config(ini: &Ini) -> Result<ExtractorConfig, Error> {
        let db_type = DbType::from_str(&ini.get(EXTRACTOR, DB_TYPE).unwrap()).unwrap();
        let extract_type =
            ExtractType::from_str(&ini.get(EXTRACTOR, "extract_type").unwrap()).unwrap();
        let url = ini.get(EXTRACTOR, URL).unwrap();

        match db_type {
            DbType::Mysql => match extract_type {
                ExtractType::Snapshot => Ok(ExtractorConfig::MysqlSnapshot {
                    url,
                    db: String::new(),
                    tb: String::new(),
                }),

                ExtractType::Cdc => Ok(ExtractorConfig::MysqlCdc {
                    url,
                    binlog_filename: ini.get(EXTRACTOR, "binlog_filename").unwrap(),
                    binlog_position: ini.getuint(EXTRACTOR, "binlog_position").unwrap().unwrap()
                        as u32,
                    server_id: ini.getuint(EXTRACTOR, "server_id").unwrap().unwrap(),
                }),

                ExtractType::CheckLog => Ok(ExtractorConfig::MysqlCheck {
                    url,
                    check_log_dir: ini.get(EXTRACTOR, CHECK_LOG_DIR).unwrap(),
                    batch_size: ini.getuint(EXTRACTOR, BATCH_SIZE).unwrap().unwrap() as usize,
                }),

                ExtractType::Struct => Ok(ExtractorConfig::MysqlStruct {
                    url,
                    db: String::new(),
                }),
            },

            DbType::Pg => match extract_type {
                ExtractType::Snapshot => Ok(ExtractorConfig::PgSnapshot {
                    url,
                    db: String::new(),
                    tb: String::new(),
                }),

                ExtractType::Cdc => Ok(ExtractorConfig::PgCdc {
                    url,
                    slot_name: ini.get(EXTRACTOR, "slot_name").unwrap(),
                    start_lsn: ini.get(EXTRACTOR, "start_lsn").unwrap(),
                    heartbeat_interval_secs: ini
                        .getuint(EXTRACTOR, "heartbeat_interval_secs")
                        .unwrap()
                        .unwrap(),
                }),

                ExtractType::CheckLog => Ok(ExtractorConfig::PgCheck {
                    url,
                    check_log_dir: ini.get(EXTRACTOR, CHECK_LOG_DIR).unwrap(),
                    batch_size: ini.getuint(EXTRACTOR, BATCH_SIZE).unwrap().unwrap() as usize,
                }),

                ExtractType::Struct => Ok(ExtractorConfig::PgStruct {
                    url,
                    db: String::new(),
                }),
            },

            DbType::Mongo => match extract_type {
                ExtractType::Snapshot => Ok(ExtractorConfig::MongoSnapshot {
                    url,
                    db: String::new(),
                    tb: String::new(),
                }),

                ExtractType::Cdc => Ok(ExtractorConfig::MongoCdc {
                    url,
                    resume_token: Self::get_optional_value(ini, EXTRACTOR, "resume_token"),
                    start_timestamp: Self::get_optional_value(ini, EXTRACTOR, "start_timestamp"),
                    source: Self::get_optional_value(ini, EXTRACTOR, "source"),
                }),

                extract_type => Err(Error::ConfigError(format!(
                    "extract type: {} not supported",
                    extract_type
                ))),
            },

            DbType::Redis => {
                let repl_port = ini.getuint(EXTRACTOR, "repl_port").unwrap().unwrap();
                match extract_type {
                    ExtractType::Snapshot => Ok(ExtractorConfig::RedisSnapshot { url, repl_port }),

                    ExtractType::Cdc => Ok(ExtractorConfig::RedisCdc {
                        url,
                        repl_port,
                        run_id: ini.get(EXTRACTOR, "run_id").unwrap(),
                        repl_offset: ini.getuint(EXTRACTOR, "repl_offset").unwrap().unwrap(),
                        heartbeat_interval_secs: ini
                            .getuint(EXTRACTOR, "heartbeat_interval_secs")
                            .unwrap()
                            .unwrap(),
                        now_db_id: ini.getint(EXTRACTOR, "now_db_id").unwrap().unwrap(),
                    }),

                    extract_type => Err(Error::ConfigError(format!(
                        "extract type: {} not supported",
                        extract_type
                    ))),
                }
            }

            db_type => Err(Error::ConfigError(format!(
                "extractor db type: {} not supported",
                db_type
            ))),
        }
    }

    fn load_sinker_config(ini: &Ini) -> Result<SinkerConfig, Error> {
        let db_type = DbType::from_str(&ini.get(SINKER, DB_TYPE).unwrap()).unwrap();
        let sink_type = SinkType::from_str(&ini.get(SINKER, "sink_type").unwrap()).unwrap();
        let url = ini.get(SINKER, URL).unwrap();
        let batch_size: usize = Self::get_optional_value(ini, SINKER, BATCH_SIZE);

        let conflict_policy_str: String = Self::get_optional_value(ini, SINKER, "conflict_policy");
        let conflict_policy = ConflictPolicyEnum::from_str(&conflict_policy_str).unwrap();

        match db_type {
            DbType::Mysql => match sink_type {
                SinkType::Write => Ok(SinkerConfig::Mysql { url, batch_size }),

                SinkType::Check => Ok(SinkerConfig::MysqlCheck {
                    url,
                    batch_size,
                    check_log_dir: ini.get(SINKER, CHECK_LOG_DIR),
                }),

                SinkType::Struct => Ok(SinkerConfig::MysqlStruct {
                    url,
                    conflict_policy,
                }),
            },

            DbType::Pg => match sink_type {
                SinkType::Write => Ok(SinkerConfig::Pg { url, batch_size }),

                SinkType::Check => Ok(SinkerConfig::PgCheck {
                    url,
                    batch_size,
                    check_log_dir: ini.get(SINKER, CHECK_LOG_DIR),
                }),

                SinkType::Struct => Ok(SinkerConfig::PgStruct {
                    url,
                    conflict_policy,
                }),
            },

            DbType::Mongo => match sink_type {
                SinkType::Write => Ok(SinkerConfig::Mongo { url, batch_size }),

                db_type => Err(Error::ConfigError(format!(
                    "sinker db type: {} not supported",
                    db_type
                ))),
            },

            DbType::Kafka => Ok(SinkerConfig::Kafka {
                url,
                batch_size,
                ack_timeout_secs: ini.getuint(SINKER, "ack_timeout_secs").unwrap().unwrap(),
                required_acks: ini.get(SINKER, "required_acks").unwrap(),
            }),

            DbType::OpenFaas => Ok(SinkerConfig::OpenFaas {
                url,
                batch_size,
                timeout_secs: ini.getuint(SINKER, "timeout_secs").unwrap().unwrap(),
            }),

            DbType::Foxlake => Ok(SinkerConfig::Foxlake {
                batch_size,
                bucket: ini.get(SINKER, "bucket").unwrap(),
                access_key: ini.get(SINKER, "access_key").unwrap(),
                secret_key: ini.get(SINKER, "secret_key").unwrap(),
                region: ini.get(SINKER, "region").unwrap(),
                root_dir: ini.get(SINKER, "root_dir").unwrap(),
            }),

            DbType::Redis => Ok(SinkerConfig::Redis {
                url,
                batch_size,
                method: Self::get_optional_value(ini, SINKER, "method"),
            }),
        }
    }

    fn load_pipeline_config(ini: &Ini) -> PipelineConfig {
        let batch_sink_interval_secs: u64 =
            Self::get_optional_value(ini, PIPELINE, "batch_sink_interval_secs");
        PipelineConfig {
            buffer_size: ini.getuint(PIPELINE, "buffer_size").unwrap().unwrap() as usize,
            parallel_size: ini.getuint(PIPELINE, "parallel_size").unwrap().unwrap() as usize,
            parallel_type: ParallelType::from_str(&ini.get(PIPELINE, "parallel_type").unwrap())
                .unwrap(),
            checkpoint_interval_secs: ini
                .getuint(PIPELINE, "checkpoint_interval_secs")
                .unwrap()
                .unwrap(),
            batch_sink_interval_secs,
        }
    }

    fn load_runtime_config(ini: &Ini) -> Result<RuntimeConfig, Error> {
        Ok(RuntimeConfig {
            log_level: ini.get(RUNTIME, "log_level").unwrap(),
            log_dir: ini.get(RUNTIME, "log_dir").unwrap(),
            log4rs_file: ini.get(RUNTIME, "log4rs_file").unwrap(),
        })
    }

    fn load_filter_config(ini: &Ini) -> Result<FilterConfig, Error> {
        Ok(FilterConfig::Rdb {
            do_dbs: ini.get(FILTER, "do_dbs").unwrap(),
            ignore_dbs: ini.get(FILTER, "ignore_dbs").unwrap(),
            do_tbs: ini.get(FILTER, "do_tbs").unwrap(),
            ignore_tbs: ini.get(FILTER, "ignore_tbs").unwrap(),
            do_events: ini.get(FILTER, "do_events").unwrap(),
        })
    }

    fn load_router_config(ini: &Ini) -> Result<RouterConfig, Error> {
        Ok(RouterConfig::Rdb {
            db_map: ini.get(ROUTER, "db_map").unwrap(),
            tb_map: ini.get(ROUTER, "tb_map").unwrap(),
            field_map: ini.get(ROUTER, "field_map").unwrap(),
        })
    }

    fn load_resumer_config(ini: &Ini) -> Result<ResumerConfig, Error> {
        let mut resume_values = HashMap::new();
        if let Some(values) = ini.get_map().unwrap().get(RESUMER) {
            resume_values = values.clone();
        }
        Ok(ResumerConfig { resume_values })
    }

    fn get_optional_value<T>(ini: &Ini, section: &str, key: &str) -> T
    where
        T: Default,
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        if let Some(value) = ini.get(section, key) {
            return value.parse::<T>().unwrap();
        }
        T::default()
    }
}
