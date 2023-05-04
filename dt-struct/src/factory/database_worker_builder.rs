use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::{
        extractor_config::ExtractorConfig, filter_config::FilterConfig,
        router_config::RouterConfig, sinker_config::SinkerConfig,
    },
    error::Error,
    meta::db_enums::DbType,
};

use crate::{
    config::struct_config::{ConflictPolicyEnum, StructConfig},
    extractor::{
        mysql::basic_mysql_extractor::MySqlStructExtractor,
        postgresql::basic_pg_extractor::PgStructExtractor,
    },
    meta::common::database_model::StructModel,
    sinker::{
        mysql::basic_mysql_sinker::MySqlStructSinker, postgresql::basic_pg_sinker::PgStructSinker,
    },
    traits::{StructExtrator, StructSinker},
    utils::database_model_ops::DatabaseModelOps,
};

pub struct StructBuilder {
    pub extractor_config: ExtractorConfig,
    pub sinker_config: SinkerConfig,
    pub filter_config: FilterConfig,
    pub router_config: RouterConfig,
    pub struct_config: StructConfig,
}

impl StructBuilder {
    pub async fn build_job<'a>(&self) -> Result<(), Error> {
        // Todo:
        // 1. step control, can migrate different kind of model by setting
        // 2. multy thread support
        // 3. more model support
        let struct_obj_queue: ConcurrentQueue<StructModel> = ConcurrentQueue::bounded(100);
        let executor_finish_flag = Arc::new(AtomicBool::new(false));

        let mut extractor = self
            .build_extractor(&struct_obj_queue, executor_finish_flag.clone())
            .await
            .unwrap();
        extractor.build_connection().await.unwrap();

        if self.struct_config.is_do_check {
            // do check. // Todo: reduce memory overhead
            let mut source_struct_models: Vec<StructModel> = vec![];

            self.check_for_database(&mut source_struct_models, &mut extractor)
                .await;

            let mut sinker = self
                .build_sinker_extractor_for_check(&struct_obj_queue, executor_finish_flag.clone())
                .await
                .unwrap();
            sinker.build_connection().await.unwrap();
            let mut sink_struct_models: Vec<StructModel> = vec![];
            self.check_for_database(&mut sink_struct_models, &mut sinker)
                .await;

            println!("\n[check result:] ");
            let check_result =
                DatabaseModelOps::check_diff(&source_struct_models, &sink_struct_models);
            if !check_result {
                println!("check finished, not the same.");
                return Err(Error::Unexpected {
                    error: String::from("check finished, not the same."),
                });
            } else {
                println!("check finished, is the same.");
            }
        } else {
            // do migration
            let mut sinker = self.build_sinker().await.unwrap();
            sinker.build_connection().await.unwrap();

            extractor.get_sequence().await.unwrap();
            tokio::join!(
                async {
                    extractor.get_table().await.unwrap();
                    extractor.get_constraint().await.unwrap();
                    extractor.get_index().await.unwrap();
                    extractor.get_comment().await.unwrap();

                    extractor.set_finished().unwrap();
                },
                async {
                    while !extractor.is_finished().unwrap() || !struct_obj_queue.is_empty() {
                        match &mut struct_obj_queue.pop() {
                            Ok(model) => {
                                let result = sinker.sink_from_queue(model).await;
                                match self.struct_config.conflict_policy {
                                    ConflictPolicyEnum::Ignore => {}
                                    ConflictPolicyEnum::Interrupt => result.unwrap(),
                                }
                            }
                            Err(e) => {
                                match e {
                                    concurrent_queue::PopError::Empty => {
                                        tokio::time::sleep(Duration::from_millis(1)).await
                                    }
                                    _ => {
                                        println!("pop from queue meet error:{}", e);
                                        panic!();
                                    }
                                }
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                        }
                    }
                }
            );
        }
        Ok(())
    }

    pub async fn build_extractor<'a>(
        &self,
        queue: &'a ConcurrentQueue<StructModel>,
        finished_flag: Arc<AtomicBool>,
    ) -> Result<Box<dyn StructExtrator + 'a + Send>, Error> {
        let extractor: Box<dyn StructExtrator + Send> = match &self.extractor_config {
            ExtractorConfig::BasicConfig { url, db_type } => match db_type {
                DbType::Mysql => Box::new(MySqlStructExtractor {
                    pool: Option::None,
                    struct_obj_queue: queue,
                    url: url.clone(),
                    db_type: db_type.clone(),
                    filter_config: self.filter_config.clone(),
                    is_finished: finished_flag,
                    is_do_check: self.struct_config.is_do_check,
                }),
                DbType::Pg => Box::new(PgStructExtractor {
                    pool: Option::None,
                    struct_obj_queue: queue,
                    url: url.clone(),
                    db_type: db_type.clone(),
                    filter_config: self.filter_config.clone(),
                    is_finished: finished_flag,
                    seq_owners: HashMap::new(),
                    is_do_check: self.struct_config.is_do_check,
                }),
                _ => {
                    return Err(Error::Unexpected {
                        error: String::from("not support extractor db type."),
                    })
                }
            },
            _ => {
                // Todo:
                panic!()
            }
        };
        Ok(extractor)
    }

    pub async fn build_sinker_extractor_for_check<'a>(
        &self,
        queue: &'a ConcurrentQueue<StructModel>,
        finished_flag: Arc<AtomicBool>,
    ) -> Result<Box<dyn StructExtrator + 'a + Send>, Error> {
        let sinker: Box<dyn StructExtrator + Send> = match &self.sinker_config {
            SinkerConfig::BasicConfig { url, db_type } => match db_type {
                DbType::Mysql => Box::new(MySqlStructExtractor {
                    pool: Option::None,
                    struct_obj_queue: queue,
                    url: url.clone(),
                    db_type: db_type.clone(),
                    filter_config: self.filter_config.clone(),
                    is_finished: finished_flag,
                    is_do_check: self.struct_config.is_do_check,
                }),
                DbType::Pg => Box::new(PgStructExtractor {
                    pool: Option::None,
                    struct_obj_queue: queue,
                    url: url.clone(),
                    db_type: db_type.clone(),
                    filter_config: self.filter_config.clone(),
                    is_finished: finished_flag,
                    seq_owners: HashMap::new(),
                    is_do_check: self.struct_config.is_do_check,
                }),
                _ => {
                    return Err(Error::Unexpected {
                        error: String::from("not support sinker db type."),
                    })
                }
            },
            _ => {
                // Todo:
                panic!()
            }
        };
        Ok(sinker)
    }

    pub async fn build_sinker(&self) -> Result<Box<dyn StructSinker + Send>, Error> {
        let sinker: Box<dyn StructSinker + Send> = match &self.sinker_config {
            SinkerConfig::BasicConfig { url: _, db_type } => match db_type {
                DbType::Mysql => Box::new(MySqlStructSinker {
                    pool: Option::None,
                    sinker_config: self.sinker_config.clone(),
                    router_config: self.router_config.clone(),
                }),
                DbType::Pg => Box::new(PgStructSinker {
                    pool: Option::None,
                    sinker_config: self.sinker_config.clone(),
                    router_config: self.router_config.clone(),
                }),
                _ => {
                    return Err(Error::Unexpected {
                        error: String::from("not support sinker db type."),
                    })
                }
            },
            _ => {
                // Todo:
                panic!()
            }
        };
        Ok(sinker)
    }

    pub async fn check_for_database<'a>(
        &self,
        models: &mut Vec<StructModel>,
        extractor: &mut Box<dyn StructExtrator + 'a + Send>,
    ) {
        models.extend_from_slice(extractor.get_sequence().await.unwrap().as_slice());
        models.extend_from_slice(extractor.get_table().await.unwrap().as_slice());
        models.extend_from_slice(extractor.get_constraint().await.unwrap().as_slice());
        models.extend_from_slice(extractor.get_index().await.unwrap().as_slice());
        models.extend_from_slice(extractor.get_comment().await.unwrap().as_slice());
    }
}
