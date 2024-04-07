use std::str::FromStr;

use crate::error::Error;

use super::{
    config_enums::{ConflictPolicyEnum, DbType, ExtractType, ParallelType, SinkType},
    data_marker_config::DataMarkerConfig,
    extractor_config::{BasicExtractorConfig, ExtractorConfig},
    filter_config::FilterConfig,
    ini_loader::IniLoader,
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
        let loader = IniLoader::new(task_config_file);

        let (extractor_basic, extractor) = Self::load_extractor_config(&loader).unwrap();
        let (sinker_basic, sinker) = Self::load_sinker_config(&loader).unwrap();
        Self {
            extractor_basic,
            extractor,
            parallelizer: Self::load_parallelizer_config(&loader),
            pipeline: Self::load_pipeline_config(&loader),
            sinker_basic,
            sinker,
            runtime: Self::load_runtime_config(&loader).unwrap(),
            filter: Self::load_filter_config(&loader).unwrap(),
            router: Self::load_router_config(&loader).unwrap(),
            resumer: Self::load_resumer_config(&loader).unwrap(),
            data_marker: Self::load_data_marker_config(&loader).unwrap(),
        }
    }

    fn load_extractor_config(
        loader: &IniLoader,
    ) -> Result<(BasicExtractorConfig, ExtractorConfig), Error> {
        let db_type_str: String = loader.get_required(EXTRACTOR, DB_TYPE);
        let extract_type_str: String = loader.get_required(EXTRACTOR, "extract_type");
        let db_type = DbType::from_str(&db_type_str).unwrap();
        let extract_type = ExtractType::from_str(&extract_type_str).unwrap();

        let url: String = loader.get_optional(EXTRACTOR, URL);
        let heartbeat_interval_secs: u64 =
            loader.get_with_default(EXTRACTOR, HEARTBEAT_INTERVAL_SECS, 10);
        let keepalive_interval_secs: u64 =
            loader.get_with_default(EXTRACTOR, KEEPALIVE_INTERVAL_SECS, 10);
        let heartbeat_tb = loader.get_optional(EXTRACTOR, HEARTBEAT_TB);

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
                    sample_interval: loader.get_with_default(EXTRACTOR, SAMPLE_INTERVAL, 1),
                },

                ExtractType::Cdc => ExtractorConfig::MysqlCdc {
                    url,
                    binlog_filename: loader.get_optional(EXTRACTOR, "binlog_filename"),
                    binlog_position: loader.get_optional(EXTRACTOR, "binlog_position"),
                    server_id: loader.get_required(EXTRACTOR, "server_id"),
                    heartbeat_interval_secs,
                    heartbeat_tb,
                },

                ExtractType::CheckLog => ExtractorConfig::MysqlCheck {
                    url,
                    check_log_dir: loader.get_required(EXTRACTOR, CHECK_LOG_DIR),
                    batch_size: loader.get_required(EXTRACTOR, BATCH_SIZE),
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
                    sample_interval: loader.get_with_default(EXTRACTOR, SAMPLE_INTERVAL, 1),
                },

                ExtractType::Cdc => ExtractorConfig::PgCdc {
                    url,
                    slot_name: loader.get_required(EXTRACTOR, "slot_name"),
                    pub_name: loader.get_optional(EXTRACTOR, "pub_name"),
                    start_lsn: loader.get_optional(EXTRACTOR, "start_lsn"),
                    keepalive_interval_secs,
                    heartbeat_interval_secs,
                    heartbeat_tb,
                    ddl_command_tb: loader.get_optional(EXTRACTOR, "ddl_command_tb"),
                },

                ExtractType::CheckLog => ExtractorConfig::PgCheck {
                    url,
                    check_log_dir: loader.get_required(EXTRACTOR, CHECK_LOG_DIR),
                    batch_size: loader.get_required(EXTRACTOR, BATCH_SIZE),
                },

                ExtractType::Struct => ExtractorConfig::PgStruct {
                    url,
                    schema: String::new(),
                },

                _ => return not_supported_err,
            },

            DbType::Mongo => {
                let app_name: String =
                    loader.get_with_default(EXTRACTOR, APP_NAME, APE_DTS.to_string());
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
                        resume_token: loader.get_optional(EXTRACTOR, "resume_token"),
                        start_timestamp: loader.get_optional(EXTRACTOR, "start_timestamp"),
                        source: loader.get_optional(EXTRACTOR, "source"),
                        heartbeat_interval_secs,
                        heartbeat_tb,
                    },

                    ExtractType::CheckLog => ExtractorConfig::MongoCheck {
                        url,
                        app_name,
                        check_log_dir: loader.get_required(EXTRACTOR, CHECK_LOG_DIR),
                        batch_size: loader.get_required(EXTRACTOR, BATCH_SIZE),
                    },

                    _ => return not_supported_err,
                }
            }

            DbType::Redis => match extract_type {
                ExtractType::Snapshot => {
                    let repl_port = loader.get_required(EXTRACTOR, "repl_port");
                    ExtractorConfig::RedisSnapshot { url, repl_port }
                }

                ExtractType::SnapshotFile => ExtractorConfig::RedisSnapshotFile {
                    file_path: loader.get_required(EXTRACTOR, "file_path"),
                },

                ExtractType::Scan => ExtractorConfig::RedisScan {
                    url,
                    statistic_type: loader.get_required(EXTRACTOR, "statistic_type"),
                    scan_count: loader.get_with_default(EXTRACTOR, "scan_count", 1000),
                },

                ExtractType::Cdc => {
                    let repl_port = loader.get_required(EXTRACTOR, "repl_port");
                    ExtractorConfig::RedisCdc {
                        url,
                        repl_port,
                        repl_id: loader.get_optional(EXTRACTOR, "repl_id"),
                        repl_offset: loader.get_optional(EXTRACTOR, "repl_offset"),
                        keepalive_interval_secs,
                        heartbeat_interval_secs,
                        heartbeat_key: loader.get_optional(EXTRACTOR, "heartbeat_key"),
                        now_db_id: loader.get_optional(EXTRACTOR, "now_db_id"),
                    }
                }

                ExtractType::Reshard => {
                    let to_node_ids = loader.get_required(EXTRACTOR, "to_node_ids");
                    ExtractorConfig::RedisReshard { url, to_node_ids }
                }

                _ => return not_supported_err,
            },

            DbType::Kafka => ExtractorConfig::Kafka {
                url,
                group: loader.get_required(EXTRACTOR, "group"),
                topic: loader.get_required(EXTRACTOR, "topic"),
                partition: loader.get_optional(EXTRACTOR, "partition"),
                offset: loader.get_optional(EXTRACTOR, "offset"),
                ack_interval_secs: loader.get_optional(EXTRACTOR, "ack_interval_secs"),
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

    fn load_sinker_config(loader: &IniLoader) -> Result<(BasicSinkerConfig, SinkerConfig), Error> {
        let db_type_str: String = loader.get_required(SINKER, DB_TYPE);
        let sink_type_str: String = loader.get_required(SINKER, "sink_type");
        let db_type = DbType::from_str(&db_type_str).unwrap();
        let sink_type = SinkType::from_str(&sink_type_str).unwrap();

        let url: String = loader.get_optional(SINKER, URL);
        let batch_size: usize = loader.get_with_default(SINKER, BATCH_SIZE, 1);

        let basic = BasicSinkerConfig {
            db_type: db_type.clone(),
            url: url.clone(),
            batch_size,
        };

        let conflict_policy_str: String = loader.get_optional(SINKER, "conflict_policy");
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
                    check_log_dir: loader.get_optional(SINKER, CHECK_LOG_DIR),
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
                    check_log_dir: loader.get_optional(SINKER, CHECK_LOG_DIR),
                },

                SinkType::Struct => SinkerConfig::PgStruct {
                    url,
                    conflict_policy,
                },

                _ => return not_supported_err,
            },

            DbType::Mongo => {
                let app_name: String =
                    loader.get_with_default(SINKER, APP_NAME, APE_DTS.to_string());
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
                        check_log_dir: loader.get_optional(SINKER, CHECK_LOG_DIR),
                    },

                    _ => return not_supported_err,
                }
            }

            DbType::Kafka => SinkerConfig::Kafka {
                url,
                batch_size,
                ack_timeout_secs: loader.get_required(SINKER, "ack_timeout_secs"),
                required_acks: loader.get_required(SINKER, "required_acks"),
            },

            DbType::Redis => match sink_type {
                SinkType::Write => SinkerConfig::Redis {
                    url,
                    batch_size,
                    method: loader.get_optional(SINKER, "method"),
                    is_cluster: loader.get_optional(SINKER, "is_cluster"),
                },

                SinkType::Statistic => SinkerConfig::RedisStatistic {
                    statistic_type: loader.get_required(SINKER, "statistic_type"),
                    data_size_threshold: loader.get_optional(SINKER, "data_size_threshold"),
                    freq_threshold: loader.get_optional(SINKER, "freq_threshold"),
                    statistic_log_dir: loader.get_optional(SINKER, "statistic_log_dir"),
                },

                SinkType::Dummy => SinkerConfig::Dummy,

                _ => return not_supported_err,
            },

            DbType::StarRocks => SinkerConfig::Starrocks {
                url,
                batch_size,
                stream_load_url: loader.get_optional(SINKER, "stream_load_url"),
            },
        };
        Ok((basic, sinker))
    }

    fn load_parallelizer_config(loader: &IniLoader) -> ParallelizerConfig {
        let parallel_type_str: String = loader.get_required(PARALLELIZER, "parallel_type");
        ParallelizerConfig {
            parallel_size: loader.get_required(PARALLELIZER, "parallel_size"),
            parallel_type: ParallelType::from_str(&parallel_type_str).unwrap(),
        }
    }

    fn load_pipeline_config(loader: &IniLoader) -> PipelineConfig {
        PipelineConfig {
            buffer_size: loader.get_required(PIPELINE, "buffer_size"),
            checkpoint_interval_secs: loader.get_with_default(
                PIPELINE,
                "checkpoint_interval_secs",
                10,
            ),
            batch_sink_interval_secs: loader.get_optional(PIPELINE, "batch_sink_interval_secs"),
            max_rps: loader.get_optional(PIPELINE, "max_rps"),
        }
    }

    fn load_runtime_config(loader: &IniLoader) -> Result<RuntimeConfig, Error> {
        Ok(RuntimeConfig {
            log_level: loader.get_with_default(RUNTIME, "log_level", "info".to_string()),
            log_dir: loader.get_with_default(RUNTIME, "log_dir", "./logs".to_string()),
            log4rs_file: loader.get_with_default(
                RUNTIME,
                "log4rs_file",
                "./log4rs.yaml".to_string(),
            ),
        })
    }

    fn load_filter_config(loader: &IniLoader) -> Result<FilterConfig, Error> {
        Ok(FilterConfig {
            do_dbs: loader.get_optional(FILTER, "do_dbs"),
            ignore_dbs: loader.get_optional(FILTER, "ignore_dbs"),
            do_tbs: loader.get_optional(FILTER, "do_tbs"),
            ignore_tbs: loader.get_optional(FILTER, "ignore_tbs"),
            do_events: loader.get_optional(FILTER, "do_events"),
            do_ddls: loader.get_optional(FILTER, "do_ddls"),
            do_structures: loader.get_with_default(FILTER, "do_structures", ASTRISK.to_string()),
            ignore_cmds: loader.get_optional(FILTER, "ignore_cmds"),
        })
    }

    fn load_router_config(loader: &IniLoader) -> Result<RouterConfig, Error> {
        Ok(RouterConfig::Rdb {
            db_map: loader.get_optional(ROUTER, "db_map"),
            tb_map: loader.get_optional(ROUTER, "tb_map"),
            col_map: loader.get_optional(ROUTER, "col_map"),
            topic_map: loader.get_optional(ROUTER, "topic_map"),
        })
    }

    fn load_resumer_config(loader: &IniLoader) -> Result<ResumerConfig, Error> {
        let mut resume_log_dir: String = loader.get_optional(RESUMER, "resume_log_dir");
        if resume_log_dir.is_empty() {
            resume_log_dir = loader.get_with_default(RUNTIME, "log_dir", "./logs".to_string());
        }

        Ok(ResumerConfig {
            tb_positions: loader.get_optional(RESUMER, "tb_positions"),
            finished_tbs: loader.get_optional(RESUMER, "finished_tbs"),
            resume_from_log: loader.get_optional(RESUMER, "resume_from_log"),
            resume_log_dir,
        })
    }

    fn load_data_marker_config(loader: &IniLoader) -> Result<Option<DataMarkerConfig>, Error> {
        if !loader.ini.sections().contains(&DATA_MARKER.to_string()) {
            return Ok(None);
        }

        Ok(Some(DataMarkerConfig {
            topo_name: loader.get_required(DATA_MARKER, "topo_name"),
            topo_nodes: loader.get_optional(DATA_MARKER, "topo_nodes"),
            src_node: loader.get_required(DATA_MARKER, "src_node"),
            dst_node: loader.get_required(DATA_MARKER, "dst_node"),
            do_nodes: loader.get_required(DATA_MARKER, "do_nodes"),
            ignore_nodes: loader.get_optional(DATA_MARKER, "ignore_nodes"),
            marker: loader.get_required(DATA_MARKER, "marker"),
        }))
    }
}
