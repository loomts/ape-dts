use std::{collections::HashMap, fmt::Debug, fs::File, io::Read, str::FromStr};

use configparser::ini::Ini;

use crate::error::Error;

use super::{
    config_enums::{ConflictPolicyEnum, DbType, ExtractType, ParallelType, SinkType},
    data_marker_config::DataMarkerConfig,
    extractor_config::{BasicExtractorConfig, ExtractorConfig},
    filter_config::FilterConfig,
    parallelizer_config::ParallelizerConfig,
    pipeline_config::PipelineConfig,
    resumer_config::ResumerConfig,
    router_config::RouterConfig,
    runtime_config::RuntimeConfig,
    sinker_config::{BasicSinkerConfig, SinkerConfig},
};

#[derive(Clone)]
pub struct TaskConfig {
    pub extractor_basic: BasicExtractorConfig,
    pub extractor: ExtractorConfig,
    pub sinker_basic: BasicSinkerConfig,
    pub sinker: SinkerConfig,
    pub runtime: RuntimeConfig,
    pub parallelizer: ParallelizerConfig,
    pub pipeline: PipelineConfig,
    pub filter: FilterConfig,
    pub router: RouterConfig,
    pub resumer: ResumerConfig,
    pub data_marker: Option<DataMarkerConfig>,
}

const EXTRACTOR: &str = "extractor";
const CHECK_LOG_DIR: &str = "check_log_dir";
const SINKER: &str = "sinker";
const DB_TYPE: &str = "db_type";
const URL: &str = "url";
const PARALLELIZER: &str = "parallelizer";
const PIPELINE: &str = "pipeline";
const RUNTIME: &str = "runtime";
const FILTER: &str = "filter";
const ROUTER: &str = "router";
const RESUMER: &str = "resumer";
const DATA_MARKER: &str = "data_marker";
const BATCH_SIZE: &str = "batch_size";
const SAMPLE_INTERVAL: &str = "sample_interval";
const ASTRISK: &str = "*";
const HEARTBEAT_INTERVAL_SECS: &str = "heartbeat_interval_secs";
const KEEPALIVE_INTERVAL_SECS: &str = "keepalive_interval_secs";
const HEARTBEAT_TB: &str = "heartbeat_tb";
const APP_NAME: &str = "app_name";
const APE_DTS: &str = "APE_DTS";

impl TaskConfig {
    pub fn new(task_config_file: &str) -> Self {
        let mut config_str = String::new();
        File::open(task_config_file)
            .unwrap()
            .read_to_string(&mut config_str)
            .unwrap();
        let mut ini = Ini::new();
        ini.read(config_str).unwrap();

        let (extractor_basic, extractor) = Self::load_extractor_config(&ini).unwrap();
        let (sinker_basic, sinker) = Self::load_sinker_config(&ini).unwrap();
        Self {
            extractor_basic,
            extractor,
            parallelizer: Self::load_parallelizer_config(&ini),
            pipeline: Self::load_pipeline_config(&ini),
            sinker_basic,
            sinker,
            runtime: Self::load_runtime_config(&ini).unwrap(),
            filter: Self::load_filter_config(&ini).unwrap(),
            router: Self::load_router_config(&ini).unwrap(),
            resumer: Self::load_resumer_config(&ini).unwrap(),
            data_marker: Self::load_data_marker_config(&ini).unwrap(),
        }
    }

    fn load_extractor_config(ini: &Ini) -> Result<(BasicExtractorConfig, ExtractorConfig), Error> {
        let db_type = DbType::from_str(&ini.get(EXTRACTOR, DB_TYPE).unwrap()).unwrap();
        let extract_type =
            ExtractType::from_str(&ini.get(EXTRACTOR, "extract_type").unwrap()).unwrap();
        let url: String = Self::get_value(ini, EXTRACTOR, URL).unwrap();
        let heartbeat_interval_secs: u64 =
            Self::get_value_with_default(ini, EXTRACTOR, HEARTBEAT_INTERVAL_SECS, 10).unwrap();
        let keepalive_interval_secs: u64 =
            Self::get_value_with_default(ini, EXTRACTOR, KEEPALIVE_INTERVAL_SECS, 10).unwrap();
        let heartbeat_tb = Self::get_value(ini, EXTRACTOR, HEARTBEAT_TB).unwrap();

        let basic = BasicExtractorConfig {
            db_type: db_type.clone(),
            extract_type: extract_type.clone(),
            url: url.clone(),
        };

        let not_supported_err = Err(Error::ConfigError(format!(
            "extract type: {} not supported",
            extract_type
        )));

        let sinker = match db_type {
            DbType::Mysql => match extract_type {
                ExtractType::Snapshot => ExtractorConfig::MysqlSnapshot {
                    url,
                    db: String::new(),
                    tb: String::new(),
                    sample_interval: Self::get_value_with_default(
                        ini,
                        EXTRACTOR,
                        SAMPLE_INTERVAL,
                        1,
                    )
                    .unwrap(),
                },

                ExtractType::Cdc => ExtractorConfig::MysqlCdc {
                    url,
                    binlog_filename: ini.get(EXTRACTOR, "binlog_filename").unwrap(),
                    binlog_position: ini.getuint(EXTRACTOR, "binlog_position").unwrap().unwrap()
                        as u32,
                    server_id: ini.getuint(EXTRACTOR, "server_id").unwrap().unwrap(),
                    heartbeat_interval_secs,
                    heartbeat_tb,
                },

                ExtractType::CheckLog => ExtractorConfig::MysqlCheck {
                    url,
                    check_log_dir: ini.get(EXTRACTOR, CHECK_LOG_DIR).unwrap(),
                    batch_size: ini.getuint(EXTRACTOR, BATCH_SIZE).unwrap().unwrap() as usize,
                },

                ExtractType::Struct => ExtractorConfig::MysqlStruct {
                    url,
                    db: String::new(),
                },

                _ => return not_supported_err,
            },

            DbType::Pg => match extract_type {
                ExtractType::Snapshot => ExtractorConfig::PgSnapshot {
                    url,
                    schema: String::new(),
                    tb: String::new(),
                    sample_interval: Self::get_value_with_default(
                        ini,
                        EXTRACTOR,
                        SAMPLE_INTERVAL,
                        1,
                    )
                    .unwrap(),
                },

                ExtractType::Cdc => ExtractorConfig::PgCdc {
                    url,
                    slot_name: ini.get(EXTRACTOR, "slot_name").unwrap(),
                    pub_name: Self::get_value(ini, EXTRACTOR, "pub_name").unwrap(),
                    start_lsn: ini.get(EXTRACTOR, "start_lsn").unwrap(),
                    keepalive_interval_secs,
                    heartbeat_interval_secs,
                    heartbeat_tb,
                    ddl_command_tb: Self::get_value(ini, EXTRACTOR, "ddl_command_tb").unwrap(),
                },

                ExtractType::CheckLog => ExtractorConfig::PgCheck {
                    url,
                    check_log_dir: ini.get(EXTRACTOR, CHECK_LOG_DIR).unwrap(),
                    batch_size: ini.getuint(EXTRACTOR, BATCH_SIZE).unwrap().unwrap() as usize,
                },

                ExtractType::Struct => ExtractorConfig::PgStruct {
                    url,
                    schema: String::new(),
                },

                _ => return not_supported_err,
            },

            DbType::Mongo => {
                let app_name: String =
                    Self::get_value_with_default(ini, EXTRACTOR, APP_NAME, APE_DTS.to_string())
                        .unwrap();
                match extract_type {
                    ExtractType::Snapshot => ExtractorConfig::MongoSnapshot {
                        url,
                        app_name,
                        db: String::new(),
                        tb: String::new(),
                    },

                    ExtractType::Cdc => ExtractorConfig::MongoCdc {
                        url,
                        app_name,
                        resume_token: Self::get_value(ini, EXTRACTOR, "resume_token").unwrap(),
                        start_timestamp: Self::get_value(ini, EXTRACTOR, "start_timestamp")
                            .unwrap(),
                        source: Self::get_value(ini, EXTRACTOR, "source").unwrap(),
                        heartbeat_interval_secs,
                        heartbeat_tb,
                    },

                    ExtractType::CheckLog => ExtractorConfig::MongoCheck {
                        url,
                        app_name,
                        check_log_dir: ini.get(EXTRACTOR, CHECK_LOG_DIR).unwrap(),
                        batch_size: ini.getuint(EXTRACTOR, BATCH_SIZE).unwrap().unwrap() as usize,
                    },

                    _ => return not_supported_err,
                }
            }

            DbType::Redis => {
                let repl_port = ini.getuint(EXTRACTOR, "repl_port").unwrap().unwrap();
                match extract_type {
                    ExtractType::Snapshot => ExtractorConfig::RedisSnapshot { url, repl_port },

                    ExtractType::SnapshotFile => ExtractorConfig::RedisSnapshotFile {
                        file_path: ini.get(EXTRACTOR, "file_path").unwrap(),
                    },

                    ExtractType::Cdc => ExtractorConfig::RedisCdc {
                        url,
                        repl_port,
                        repl_id: Self::get_value(ini, EXTRACTOR, "repl_id").unwrap(),
                        repl_offset: ini.getuint(EXTRACTOR, "repl_offset").unwrap().unwrap(),
                        keepalive_interval_secs,
                        heartbeat_interval_secs,
                        heartbeat_key: Self::get_value(ini, EXTRACTOR, "heartbeat_key").unwrap(),
                        now_db_id: ini.getint(EXTRACTOR, "now_db_id").unwrap().unwrap(),
                    },

                    _ => return not_supported_err,
                }
            }

            DbType::Kafka => ExtractorConfig::Kafka {
                url,
                group: ini.get(EXTRACTOR, "group").unwrap(),
                topic: ini.get(EXTRACTOR, "topic").unwrap(),
                partition: Self::get_value(ini, EXTRACTOR, "partition").unwrap(),
                offset: Self::get_value(ini, EXTRACTOR, "offset").unwrap(),
                ack_interval_secs: Self::get_value(ini, EXTRACTOR, "ack_interval_secs").unwrap(),
            },

            db_type => {
                return Err(Error::ConfigError(format!(
                    "extractor db type: {} not supported",
                    db_type
                )))
            }
        };
        Ok((basic, sinker))
    }

    fn load_sinker_config(ini: &Ini) -> Result<(BasicSinkerConfig, SinkerConfig), Error> {
        let db_type = DbType::from_str(&ini.get(SINKER, DB_TYPE).unwrap()).unwrap();
        let sink_type = SinkType::from_str(&ini.get(SINKER, "sink_type").unwrap()).unwrap();
        let url: String = Self::get_value(ini, SINKER, URL).unwrap();
        let batch_size: usize = Self::get_value(ini, SINKER, BATCH_SIZE).unwrap();

        let basic = BasicSinkerConfig {
            db_type: db_type.clone(),
            url: url.clone(),
            batch_size,
        };

        let conflict_policy_str: String = Self::get_value(ini, SINKER, "conflict_policy").unwrap();
        let conflict_policy = ConflictPolicyEnum::from_str(&conflict_policy_str).unwrap();

        let not_supported_err = Err(Error::ConfigError(format!(
            "sinker db type: {} not supported",
            db_type
        )));

        let sinker = match db_type {
            DbType::Mysql => match sink_type {
                SinkType::Write => SinkerConfig::Mysql { url, batch_size },

                SinkType::Check => SinkerConfig::MysqlCheck {
                    url,
                    batch_size,
                    check_log_dir: Self::get_value(ini, SINKER, CHECK_LOG_DIR).unwrap(),
                },

                SinkType::Struct => SinkerConfig::MysqlStruct {
                    url,
                    conflict_policy,
                },

                _ => return not_supported_err,
            },

            DbType::Pg => match sink_type {
                SinkType::Write => SinkerConfig::Pg { url, batch_size },

                SinkType::Check => SinkerConfig::PgCheck {
                    url,
                    batch_size,
                    check_log_dir: Self::get_value(ini, SINKER, CHECK_LOG_DIR).unwrap(),
                },

                SinkType::Struct => SinkerConfig::PgStruct {
                    url,
                    conflict_policy,
                },

                _ => return not_supported_err,
            },

            DbType::Mongo => {
                let app_name: String =
                    Self::get_value_with_default(ini, SINKER, APP_NAME, APE_DTS.to_string())
                        .unwrap();
                match sink_type {
                    SinkType::Write => SinkerConfig::Mongo {
                        url,
                        app_name,
                        batch_size,
                    },

                    SinkType::Check => SinkerConfig::MongoCheck {
                        url,
                        app_name,
                        batch_size,
                        check_log_dir: Self::get_value(ini, SINKER, CHECK_LOG_DIR).unwrap(),
                    },

                    _ => return not_supported_err,
                }
            }

            DbType::Kafka => SinkerConfig::Kafka {
                url,
                batch_size,
                ack_timeout_secs: ini.getuint(SINKER, "ack_timeout_secs").unwrap().unwrap(),
                required_acks: ini.get(SINKER, "required_acks").unwrap(),
            },

            DbType::Redis => match sink_type {
                SinkType::Write => SinkerConfig::Redis {
                    url,
                    batch_size,
                    method: Self::get_value(ini, SINKER, "method").unwrap(),
                    is_cluster: Self::get_value(ini, SINKER, "is_cluster").unwrap(),
                },

                SinkType::Statistic => SinkerConfig::RedisStatistic {
                    data_size_threshold: Self::get_value(ini, SINKER, "data_size_threshold")
                        .unwrap(),
                    statistic_log_dir: Self::get_value(ini, SINKER, "statistic_log_dir").unwrap(),
                },

                _ => return not_supported_err,
            },

            DbType::StarRocks => SinkerConfig::Starrocks {
                url,
                batch_size,
                stream_load_url: Self::get_value(ini, SINKER, "stream_load_url").unwrap(),
            },
        };
        Ok((basic, sinker))
    }

    fn load_parallelizer_config(ini: &Ini) -> ParallelizerConfig {
        // compatible with older versions, Paralleizer settings are set under the pipeline section
        ParallelizerConfig {
            parallel_size: ini.getuint(PARALLELIZER, "parallel_size").unwrap().unwrap() as usize,
            parallel_type: ParallelType::from_str(&ini.get(PARALLELIZER, "parallel_type").unwrap())
                .unwrap(),
        }
    }

    fn load_pipeline_config(ini: &Ini) -> PipelineConfig {
        let buffer_size = ini.getuint(PIPELINE, "buffer_size").unwrap().unwrap() as usize;
        let batch_sink_interval_secs: u64 =
            Self::get_value(ini, PIPELINE, "batch_sink_interval_secs").unwrap();
        let checkpoint_interval_secs = ini
            .getuint(PIPELINE, "checkpoint_interval_secs")
            .unwrap()
            .unwrap_or(1);
        let max_rps = Self::get_value(ini, PIPELINE, "max_rps").unwrap();

        PipelineConfig {
            buffer_size,
            checkpoint_interval_secs,
            batch_sink_interval_secs,
            max_rps,
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
        Ok(FilterConfig {
            do_dbs: ini.get(FILTER, "do_dbs").unwrap(),
            ignore_dbs: ini.get(FILTER, "ignore_dbs").unwrap(),
            do_tbs: ini.get(FILTER, "do_tbs").unwrap(),
            ignore_tbs: ini.get(FILTER, "ignore_tbs").unwrap(),
            do_events: ini.get(FILTER, "do_events").unwrap(),
            do_ddls: Self::get_value(ini, FILTER, "do_ddls").unwrap(),
            do_structures: Self::get_value_with_default(
                ini,
                FILTER,
                "do_structures",
                ASTRISK.to_string(),
            )
            .unwrap(),
        })
    }

    fn load_router_config(ini: &Ini) -> Result<RouterConfig, Error> {
        Ok(RouterConfig::Rdb {
            db_map: Self::get_value(ini, ROUTER, "db_map").unwrap(),
            tb_map: Self::get_value(ini, ROUTER, "tb_map").unwrap(),
            col_map: Self::get_value(ini, ROUTER, "col_map").unwrap(),
            topic_map: Self::get_value(ini, ROUTER, "topic_map").unwrap(),
        })
    }

    fn load_resumer_config(ini: &Ini) -> Result<ResumerConfig, Error> {
        let mut resume_values = HashMap::new();
        if let Some(values) = ini.get_map().unwrap().get(RESUMER) {
            resume_values = values.clone();
        }
        Ok(ResumerConfig { resume_values })
    }

    fn load_data_marker_config(ini: &Ini) -> Result<Option<DataMarkerConfig>, Error> {
        if !ini.sections().contains(&DATA_MARKER.to_string()) {
            return Ok(None);
        }

        Ok(Some(DataMarkerConfig {
            topo_name: ini.get(DATA_MARKER, "topo_name").unwrap(),
            topo_nodes: ini.get(DATA_MARKER, "topo_nodes").unwrap(),
            src_node: ini.get(DATA_MARKER, "src_node").unwrap(),
            dst_node: ini.get(DATA_MARKER, "dst_node").unwrap(),
            do_nodes: ini.get(DATA_MARKER, "do_nodes").unwrap(),
            ignore_nodes: Self::get_value(ini, DATA_MARKER, "ignore_nodes").unwrap(),
            marker: Self::get_value(ini, DATA_MARKER, "marker").unwrap(),
        }))
    }

    fn get_value_with_default<T>(
        ini: &Ini,
        section: &str,
        key: &str,
        default: T,
    ) -> Result<T, <T as FromStr>::Err>
    where
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        if let Some(value) = ini.get(section, key) {
            if !value.is_empty() {
                return value.parse::<T>();
            }
        }
        Ok(default)
    }

    fn get_value<T>(ini: &Ini, section: &str, key: &str) -> Result<T, <T as FromStr>::Err>
    where
        T: Default,
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        Self::get_value_with_default(ini, section, key, T::default())
    }
}
