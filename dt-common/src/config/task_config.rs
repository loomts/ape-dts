use std::{any::type_name, collections::HashMap, fmt::Debug, fs::File, io::Read, str::FromStr};

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

// sections
const EXTRACTOR: &str = "extractor";
const SINKER: &str = "sinker";
const PIPELINE: &str = "pipeline";
const PARALLELIZER: &str = "parallelizer";
const RUNTIME: &str = "runtime";
const FILTER: &str = "filter";
const ROUTER: &str = "router";
const RESUMER: &str = "resumer";
const DATA_MARKER: &str = "data_marker";
// keys
const CHECK_LOG_DIR: &str = "check_log_dir";
const DB_TYPE: &str = "db_type";
const URL: &str = "url";
const BATCH_SIZE: &str = "batch_size";
const SAMPLE_INTERVAL: &str = "sample_interval";
const HEARTBEAT_INTERVAL_SECS: &str = "heartbeat_interval_secs";
const KEEPALIVE_INTERVAL_SECS: &str = "keepalive_interval_secs";
const HEARTBEAT_TB: &str = "heartbeat_tb";
const APP_NAME: &str = "app_name";
// default values
const APE_DTS: &str = "APE_DTS";
const ASTRISK: &str = "*";

impl TaskConfig {
    pub fn new(task_config_file: &str) -> Self {
        let mut config_str = String::new();
        File::open(task_config_file)
            .expect("failed to open task_config file")
            .read_to_string(&mut config_str)
            .expect("failed to read task_config content");
        let mut ini = Ini::new();
        ini.read(config_str)
            .expect("failed to read task_config content as ini");

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
        let db_type_str: String = Self::get_required(ini, EXTRACTOR, DB_TYPE);
        let extract_type_str: String = Self::get_required(ini, EXTRACTOR, "extract_type");
        let db_type = DbType::from_str(&db_type_str).unwrap();
        let extract_type = ExtractType::from_str(&extract_type_str).unwrap();

        let url: String = Self::get_optional(ini, EXTRACTOR, URL);
        let heartbeat_interval_secs: u64 =
            Self::get_with_default(ini, EXTRACTOR, HEARTBEAT_INTERVAL_SECS, 10);
        let keepalive_interval_secs: u64 =
            Self::get_with_default(ini, EXTRACTOR, KEEPALIVE_INTERVAL_SECS, 10);
        let heartbeat_tb = Self::get_optional(ini, EXTRACTOR, HEARTBEAT_TB);

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
                    sample_interval: Self::get_with_default(ini, EXTRACTOR, SAMPLE_INTERVAL, 1),
                },

                ExtractType::Cdc => ExtractorConfig::MysqlCdc {
                    url,
                    binlog_filename: Self::get_optional(ini, EXTRACTOR, "binlog_filename"),
                    binlog_position: Self::get_optional(ini, EXTRACTOR, "binlog_position"),
                    server_id: Self::get_required(ini, EXTRACTOR, "server_id"),
                    heartbeat_interval_secs,
                    heartbeat_tb,
                },

                ExtractType::CheckLog => ExtractorConfig::MysqlCheck {
                    url,
                    check_log_dir: Self::get_required(ini, EXTRACTOR, CHECK_LOG_DIR),
                    batch_size: Self::get_required(ini, EXTRACTOR, BATCH_SIZE),
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
                    sample_interval: Self::get_with_default(ini, EXTRACTOR, SAMPLE_INTERVAL, 1),
                },

                ExtractType::Cdc => ExtractorConfig::PgCdc {
                    url,
                    slot_name: Self::get_required(ini, EXTRACTOR, "slot_name"),
                    pub_name: Self::get_optional(ini, EXTRACTOR, "pub_name"),
                    start_lsn: Self::get_optional(ini, EXTRACTOR, "start_lsn"),
                    keepalive_interval_secs,
                    heartbeat_interval_secs,
                    heartbeat_tb,
                    ddl_command_tb: Self::get_optional(ini, EXTRACTOR, "ddl_command_tb"),
                },

                ExtractType::CheckLog => ExtractorConfig::PgCheck {
                    url,
                    check_log_dir: Self::get_required(ini, EXTRACTOR, CHECK_LOG_DIR),
                    batch_size: Self::get_required(ini, EXTRACTOR, BATCH_SIZE),
                },

                ExtractType::Struct => ExtractorConfig::PgStruct {
                    url,
                    schema: String::new(),
                },

                _ => return not_supported_err,
            },

            DbType::Mongo => {
                let app_name: String =
                    Self::get_with_default(ini, EXTRACTOR, APP_NAME, APE_DTS.to_string());
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
                        resume_token: Self::get_optional(ini, EXTRACTOR, "resume_token"),
                        start_timestamp: Self::get_optional(ini, EXTRACTOR, "start_timestamp"),
                        source: Self::get_optional(ini, EXTRACTOR, "source"),
                        heartbeat_interval_secs,
                        heartbeat_tb,
                    },

                    ExtractType::CheckLog => ExtractorConfig::MongoCheck {
                        url,
                        app_name,
                        check_log_dir: Self::get_required(ini, EXTRACTOR, CHECK_LOG_DIR),
                        batch_size: Self::get_required(ini, EXTRACTOR, BATCH_SIZE),
                    },

                    _ => return not_supported_err,
                }
            }

            DbType::Redis => {
                let repl_port = Self::get_required(ini, EXTRACTOR, "repl_port");
                match extract_type {
                    ExtractType::Snapshot => ExtractorConfig::RedisSnapshot { url, repl_port },

                    ExtractType::SnapshotFile => ExtractorConfig::RedisSnapshotFile {
                        file_path: Self::get_required(ini, EXTRACTOR, "file_path"),
                    },

                    ExtractType::Cdc => ExtractorConfig::RedisCdc {
                        url,
                        repl_port,
                        repl_id: Self::get_optional(ini, EXTRACTOR, "repl_id"),
                        repl_offset: Self::get_optional(ini, EXTRACTOR, "repl_offset"),
                        keepalive_interval_secs,
                        heartbeat_interval_secs,
                        heartbeat_key: Self::get_optional(ini, EXTRACTOR, "heartbeat_key"),
                        now_db_id: Self::get_optional(ini, EXTRACTOR, "now_db_id"),
                    },

                    _ => return not_supported_err,
                }
            }

            DbType::Kafka => ExtractorConfig::Kafka {
                url,
                group: Self::get_required(ini, EXTRACTOR, "group"),
                topic: Self::get_required(ini, EXTRACTOR, "topic"),
                partition: Self::get_optional(ini, EXTRACTOR, "partition"),
                offset: Self::get_optional(ini, EXTRACTOR, "offset"),
                ack_interval_secs: Self::get_optional(ini, EXTRACTOR, "ack_interval_secs"),
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
        let db_type_str: String = Self::get_required(ini, SINKER, DB_TYPE);
        let sink_type_str: String = Self::get_required(ini, SINKER, "sink_type");
        let db_type = DbType::from_str(&db_type_str).unwrap();
        let sink_type = SinkType::from_str(&sink_type_str).unwrap();

        let url: String = Self::get_optional(ini, SINKER, URL);
        let batch_size: usize = Self::get_required(ini, SINKER, BATCH_SIZE);

        let basic = BasicSinkerConfig {
            db_type: db_type.clone(),
            url: url.clone(),
            batch_size,
        };

        let conflict_policy_str: String = Self::get_optional(ini, SINKER, "conflict_policy");
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
                    check_log_dir: Self::get_optional(ini, SINKER, CHECK_LOG_DIR),
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
                    check_log_dir: Self::get_optional(ini, SINKER, CHECK_LOG_DIR),
                },

                SinkType::Struct => SinkerConfig::PgStruct {
                    url,
                    conflict_policy,
                },

                _ => return not_supported_err,
            },

            DbType::Mongo => {
                let app_name: String =
                    Self::get_with_default(ini, SINKER, APP_NAME, APE_DTS.to_string());
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
                        check_log_dir: Self::get_optional(ini, SINKER, CHECK_LOG_DIR),
                    },

                    _ => return not_supported_err,
                }
            }

            DbType::Kafka => SinkerConfig::Kafka {
                url,
                batch_size,
                ack_timeout_secs: Self::get_required(ini, SINKER, "ack_timeout_secs"),
                required_acks: Self::get_required(ini, SINKER, "required_acks"),
            },

            DbType::Redis => match sink_type {
                SinkType::Write => SinkerConfig::Redis {
                    url,
                    batch_size,
                    method: Self::get_optional(ini, SINKER, "method"),
                    is_cluster: Self::get_optional(ini, SINKER, "is_cluster"),
                },

                SinkType::Statistic => SinkerConfig::RedisStatistic {
                    data_size_threshold: Self::get_optional(ini, SINKER, "data_size_threshold"),
                    statistic_log_dir: Self::get_optional(ini, SINKER, "statistic_log_dir"),
                },

                _ => return not_supported_err,
            },

            DbType::StarRocks => SinkerConfig::Starrocks {
                url,
                batch_size,
                stream_load_url: Self::get_optional(ini, SINKER, "stream_load_url"),
            },
        };
        Ok((basic, sinker))
    }

    fn load_parallelizer_config(ini: &Ini) -> ParallelizerConfig {
        let parallel_type_str: String = Self::get_required(ini, PARALLELIZER, "parallel_type");
        ParallelizerConfig {
            parallel_size: Self::get_required(ini, PARALLELIZER, "parallel_size"),
            parallel_type: ParallelType::from_str(&parallel_type_str).unwrap(),
        }
    }

    fn load_pipeline_config(ini: &Ini) -> PipelineConfig {
        PipelineConfig {
            buffer_size: Self::get_required(ini, PIPELINE, "buffer_size"),
            checkpoint_interval_secs: Self::get_with_default(
                ini,
                PIPELINE,
                "checkpoint_interval_secs",
                10,
            ),
            batch_sink_interval_secs: Self::get_optional(ini, PIPELINE, "batch_sink_interval_secs"),
            max_rps: Self::get_optional(ini, PIPELINE, "max_rps"),
        }
    }

    fn load_runtime_config(ini: &Ini) -> Result<RuntimeConfig, Error> {
        Ok(RuntimeConfig {
            log_level: Self::get_with_default(ini, RUNTIME, "log_level", "info".to_string()),
            log_dir: Self::get_with_default(ini, RUNTIME, "log_dir", "./log4rs.yaml".to_string()),
            log4rs_file: Self::get_with_default(ini, RUNTIME, "log4rs_file", "./logs".to_string()),
        })
    }

    fn load_filter_config(ini: &Ini) -> Result<FilterConfig, Error> {
        Ok(FilterConfig {
            do_dbs: Self::get_optional(ini, FILTER, "do_dbs"),
            ignore_dbs: Self::get_optional(ini, FILTER, "ignore_dbs"),
            do_tbs: Self::get_optional(ini, FILTER, "do_tbs"),
            ignore_tbs: Self::get_optional(ini, FILTER, "ignore_tbs"),
            do_events: Self::get_optional(ini, FILTER, "do_events"),
            do_ddls: Self::get_optional(ini, FILTER, "do_ddls"),
            do_structures: Self::get_with_default(
                ini,
                FILTER,
                "do_structures",
                ASTRISK.to_string(),
            ),
        })
    }

    fn load_router_config(ini: &Ini) -> Result<RouterConfig, Error> {
        Ok(RouterConfig::Rdb {
            db_map: Self::get_optional(ini, ROUTER, "db_map"),
            tb_map: Self::get_optional(ini, ROUTER, "tb_map"),
            col_map: Self::get_optional(ini, ROUTER, "col_map"),
            topic_map: Self::get_optional(ini, ROUTER, "topic_map"),
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
            topo_name: Self::get_required(ini, DATA_MARKER, "topo_name"),
            topo_nodes: Self::get_optional(ini, DATA_MARKER, "topo_nodes"),
            src_node: Self::get_required(ini, DATA_MARKER, "src_node"),
            dst_node: Self::get_required(ini, DATA_MARKER, "dst_node"),
            do_nodes: Self::get_required(ini, DATA_MARKER, "do_nodes"),
            ignore_nodes: Self::get_optional(ini, DATA_MARKER, "ignore_nodes"),
            marker: Self::get_required(ini, DATA_MARKER, "marker"),
        }))
    }

    fn get_required<T>(ini: &Ini, section: &str, key: &str) -> T
    where
        T: FromStr,
    {
        if let Some(value) = ini.get(section, key) {
            if !value.is_empty() {
                return Self::parse_value(section, key, &value).unwrap();
            }
        }
        panic!("config [{}].{} does not exist or is empty", section, key);
    }

    fn get_optional<T>(ini: &Ini, section: &str, key: &str) -> T
    where
        T: Default,
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        Self::get_with_default(ini, section, key, T::default())
    }

    fn get_with_default<T>(ini: &Ini, section: &str, key: &str, default: T) -> T
    where
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        if let Some(value) = ini.get(section, key) {
            return Self::parse_value(section, key, &value).unwrap();
        }
        default
    }

    fn parse_value<T>(section: &str, key: &str, value: &str) -> Result<T, Error>
    where
        T: FromStr,
    {
        match value.parse::<T>() {
            Ok(v) => Ok(v),
            Err(_) => Err(Error::ConfigError(format!(
                "config [{}].{}={}, can not be parsed as {}",
                section,
                key,
                value,
                type_name::<T>(),
            ))),
        }
    }
}
