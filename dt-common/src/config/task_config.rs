use std::{collections::HashMap, fmt::Debug, fs::File, io::Read, str::FromStr};

use configparser::ini::Ini;

use crate::error::Error;

use super::{
    config_enums::{ConflictPolicyEnum, DbType, ExtractType, ParallelType, PipelineType, SinkType},
    extractor_config::ExtractorConfig,
    filter_config::FilterConfig,
    parallelizer_config::ParallelizerConfig,
    pipeline_config::{ExtraConfig, PipelineConfig},
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
    pub parallelizer: ParallelizerConfig,
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
const PARALLELIZER: &str = "parallelizer";
const PIPELINE: &str = "pipeline";
const PIPELINE_TYPE: &str = "type";
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
            parallelizer: Self::load_paralleizer_config(&ini),
            sinker: Self::load_sinker_config(&ini).unwrap(),
            pipeline: Self::load_pipeline_config(&ini),
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

                ExtractType::Basic => Ok(ExtractorConfig::Basic { url, db_type }),
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

                ExtractType::Basic => Ok(ExtractorConfig::Basic { url, db_type }),
            },

            DbType::Mongo => match extract_type {
                ExtractType::Snapshot => Ok(ExtractorConfig::MongoSnapshot {
                    url,
                    db: String::new(),
                    tb: String::new(),
                }),

                ExtractType::Cdc => {
                    let start_timestamp: i64 = match ini.getint(EXTRACTOR, "start_timestamp") {
                        Ok(ts_option) => {
                            if let Some(ts) = ts_option {
                                ts
                            } else {
                                0
                            }
                        }
                        Err(_) => 0,
                    };
                    let resume_token: String = match ini.get(EXTRACTOR, "resume_token") {
                        Some(val) => val,
                        None => String::from(""),
                    };
                    Ok(ExtractorConfig::MongoCdc {
                        url,
                        resume_token,
                        start_timestamp,
                    })
                }

                ExtractType::Basic => Ok(ExtractorConfig::Basic { url, db_type }),

                _ => Err(Error::Unexpected {
                    error: "extractor db type not supported".to_string(),
                }),
            },

            _ => Err(Error::Unexpected {
                error: "extractor db type not supported".to_string(),
            }),
        }
    }

    fn load_sinker_config(ini: &Ini) -> Result<SinkerConfig, Error> {
        let db_type = DbType::from_str(&ini.get(SINKER, DB_TYPE).unwrap()).unwrap();
        let sink_type = SinkType::from_str(&ini.get(SINKER, "sink_type").unwrap()).unwrap();
        let url = ini.get(SINKER, URL).unwrap();
        let batch_size: usize = Self::get_optional_value(ini, SINKER, BATCH_SIZE);

        let conflict_policy_str: String = Self::get_optional_value(ini, SINKER, "conflict_policy");
        let conflict_policy = ConflictPolicyEnum::from_str(&conflict_policy_str)
            .unwrap_or(ConflictPolicyEnum::Interrupt);

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

                SinkType::Basic => Ok(SinkerConfig::Basic { url, db_type }),
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

                SinkType::Basic => Ok(SinkerConfig::Basic { url, db_type }),
            },

            DbType::Mongo => match sink_type {
                SinkType::Write => Ok(SinkerConfig::Mongo { url, batch_size }),

                SinkType::Basic => Ok(SinkerConfig::Basic { url, db_type }),

                _ => Err(Error::Unexpected {
                    error: "sinker db type not supported".to_string(),
                }),
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
        }
    }

    fn load_paralleizer_config(ini: &Ini) -> ParallelizerConfig {
        ParallelizerConfig {
            parallel_size: ini.getuint(PARALLELIZER, "parallel_size").unwrap().unwrap() as usize,
            parallel_type: ParallelType::from_str(&ini.get(PARALLELIZER, "parallel_type").unwrap())
                .unwrap(),
        }
    }

    fn load_pipeline_config(ini: &Ini) -> PipelineConfig {
        let pipeline_type: PipelineType = match &ini.get(PIPELINE, PIPELINE_TYPE) {
            Some(t) => PipelineType::from_str(t).unwrap(),
            None => PipelineType::Basic,
        };

        let buffer_size = ini.getuint(PIPELINE, "buffer_size").unwrap().unwrap() as usize;
        let batch_sink_interval_secs: u64 =
            Self::get_optional_value(ini, PIPELINE, "batch_sink_interval_secs");
        let checkpoint_interval_secs = ini
            .getuint(PIPELINE, "checkpoint_interval_secs")
            .unwrap()
            .unwrap_or(1);

        let extra_config = match pipeline_type {
            PipelineType::Transaction => ExtraConfig::Transaction {
                transaction_db: ini.get(PIPELINE, "transaction_db").unwrap(),
                transaction_table: ini.get(PIPELINE, "transaction_table").unwrap(),
                transaction_express: ini.get(PIPELINE, "transaction_express").unwrap(),
                transaction_command: ini.get(PIPELINE, "transaction_command").unwrap(),
                white_nodes: ini.get(PIPELINE, "white_nodes").unwrap(),
                black_nodes: ini.get(PIPELINE, "black_nodes").unwrap(),
            },
            _ => ExtraConfig::Basic {},
        };

        PipelineConfig {
            buffer_size,
            checkpoint_interval_secs,
            batch_sink_interval_secs,
            extra_config,
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

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use crate::utils::work_dir_util::WorkDirUtil;

    use super::*;

    #[test]
    fn task_config_new_test() {
        let project_root = WorkDirUtil::get_project_root().unwrap();

        let path_name = format!("{}/dt-common/src/test/config", project_root);
        println!("path_name:{}", path_name);

        for entry in fs::read_dir(PathBuf::from(path_name)).unwrap() {
            if entry.is_err() {
                continue;
            }
            let dir_entry = entry.unwrap();
            println!(
                "begin check config: {}",
                dir_entry.file_name().to_string_lossy()
            );
            TaskConfig::new(&dir_entry.path().to_string_lossy().to_string());
        }
        assert!(true)
    }

    fn mock_props(confs: &str) -> PipelineConfig {
        let mut inis = Ini::new();
        inis.read(String::from(confs)).unwrap();
        TaskConfig::load_pipeline_config(&inis)
    }

    #[test]
    fn load_pipeline_config_test() {
        let mut conf: &str = "
        [pipeline]
        type=basic

        buffer_size=4
        checkpoint_interval_secs=1
        batch_sink_interval_secs=10
        ";
        let mut pipeline_config = mock_props(conf);
        assert_eq!(
            pipeline_config.get_pipeline_type().to_string(),
            PipelineType::Basic.to_string()
        );
        assert_eq!(pipeline_config.buffer_size, 4);
        assert_eq!(pipeline_config.checkpoint_interval_secs, 1);
        assert_eq!(pipeline_config.batch_sink_interval_secs, 10);

        conf = "
        [pipeline]
        type=basic

        buffer_size=4
        ";
        pipeline_config = mock_props(conf);
        assert_eq!(
            pipeline_config.get_pipeline_type().to_string(),
            PipelineType::Basic.to_string()
        );
        assert_eq!(pipeline_config.buffer_size, 4);
        assert_eq!(pipeline_config.checkpoint_interval_secs, 1);
        assert_eq!(pipeline_config.batch_sink_interval_secs, 0);

        conf = "
        [pipeline]
        type=transaction
        buffer_size=4

        transaction_db=ape_dt
        transaction_table=ape_dt_topo1_node1_node2
        transaction_express=ape_dt_(?<topology>.*)_(?<source>.*)_(?<sink>.*)
        transaction_command=update ape_dt_topo1_node1_node2 set n = n + 1;
        topology_key=topo1
        white_nodes=4,5,6
        black_nodes=1,2,3
        ";
        pipeline_config = mock_props(conf);
        assert_eq!(
            pipeline_config.get_pipeline_type().to_string(),
            PipelineType::Transaction.to_string()
        );
        match pipeline_config.extra_config {
            ExtraConfig::Basic {} => assert!(false),
            ExtraConfig::Transaction {
                transaction_db,
                transaction_table,
                transaction_express,
                transaction_command,
                white_nodes,
                black_nodes,
            } => {
                assert_eq!(transaction_db, "ape_dt");
                assert_eq!(transaction_table, "ape_dt_topo1_node1_node2");
                assert_eq!(
                    transaction_express,
                    "ape_dt_(?<topology>.*)_(?<source>.*)_(?<sink>.*)"
                );
                assert_eq!(
                    transaction_command,
                    "update ape_dt_topo1_node1_node2 set n = n + 1"
                );
                assert_eq!(white_nodes, "4,5,6");
                assert_eq!(black_nodes, "1,2,3");
            }
        }
    }
}
