use std::{collections::HashMap, sync::atomic::AtomicBool};

use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::{
        config_enums::DbType, extractor_config::ExtractorConfig, sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    error::Error,
};
use dt_connector::extractor::rdb_filter::RdbFilter;
use dt_meta::{dt_data::DtData, struct_meta::database_model::StructModel};
use dt_task::extractor_util::ExtractorUtil;

use super::rdb_test_runner::RdbTestRunner;

pub struct RdbStructTestRunner {
    base: RdbTestRunner,
}

impl RdbStructTestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        let base = RdbTestRunner::new(relative_test_dir).await.unwrap();
        Ok(Self { base })
    }

    pub async fn run_mysql_struct_test(&mut self) -> Result<(), Error> {
        self.base.execute_test_ddl_sqls().await?;
        self.base.base.start_task().await?;

        let (src_url, dst_url, log_level, dbs, mut filter, buffer, shut_down) =
            self.build_extractor_parameters().await;

        for db in dbs.iter() {
            if filter.filter_db(db) {
                continue;
            }

            let mut src_extractor = ExtractorUtil::create_mysql_struct_extractor(
                &src_url,
                db,
                &buffer,
                filter.clone(),
                &log_level,
                &shut_down,
            )
            .await?;

            let mut dst_extractor = ExtractorUtil::create_mysql_struct_extractor(
                &dst_url,
                db,
                &buffer,
                filter.clone(),
                &log_level,
                &shut_down,
            )
            .await?;

            let src_models = src_extractor.get_table().await.unwrap();
            let dst_models = dst_extractor.get_table().await.unwrap();
            self.compare_models(&src_models, &dst_models);

            let src_models = src_extractor.get_index().await.unwrap();
            let dst_models = dst_extractor.get_index().await.unwrap();
            self.compare_models(&src_models, &dst_models);
        }

        Ok(())
    }

    pub async fn run_pg_struct_test(&mut self) -> Result<(), Error> {
        self.base.execute_test_ddl_sqls().await?;
        self.base.base.start_task().await?;

        let (src_url, dst_url, log_level, dbs, mut filter, buffer, shut_down) =
            self.build_extractor_parameters().await;

        for db in dbs.iter() {
            if filter.filter_db(db) {
                continue;
            }

            let mut src_extractor = ExtractorUtil::create_pg_struct_extractor(
                &src_url,
                db,
                &buffer,
                filter.clone(),
                &log_level,
                &shut_down,
            )
            .await?;

            let mut dst_extractor = ExtractorUtil::create_pg_struct_extractor(
                &dst_url,
                db,
                &buffer,
                filter.clone(),
                &log_level,
                &shut_down,
            )
            .await?;

            let src_models = src_extractor.get_table().await.unwrap();
            let dst_models = dst_extractor.get_table().await.unwrap();
            self.compare_models(&src_models, &dst_models);

            let src_models = src_extractor.get_index().await.unwrap();
            let dst_models = dst_extractor.get_index().await.unwrap();
            self.compare_models(&src_models, &dst_models);

            let src_models = src_extractor.get_sequence().await.unwrap();
            let dst_models = dst_extractor.get_sequence().await.unwrap();
            self.compare_models(&src_models.0, &dst_models.0);
            self.compare_models(&src_models.1, &dst_models.1);

            let src_models = src_extractor.get_constraint().await.unwrap();
            let dst_models = dst_extractor.get_constraint().await.unwrap();
            self.compare_models(&src_models, &dst_models);

            let src_models = src_extractor.get_comment().await.unwrap();
            let dst_models = dst_extractor.get_comment().await.unwrap();
            self.compare_models(&src_models, &dst_models);
        }

        Ok(())
    }

    async fn build_extractor_parameters(
        &self,
    ) -> (
        String,
        String,
        String,
        Vec<String>,
        RdbFilter,
        ConcurrentQueue<DtData>,
        AtomicBool,
    ) {
        let config = TaskConfig::new(&self.base.base.task_config_file);
        let filter = RdbFilter::from_config(&config.filter).unwrap();
        let buffer: ConcurrentQueue<DtData> = ConcurrentQueue::bounded(10000);
        let shut_down = AtomicBool::new(false);
        let log_level = "info".to_string();

        let (src_url, dbs) = match &config.extractor {
            ExtractorConfig::MysqlBasic { url, .. } => (
                url.to_string(),
                ExtractorUtil::list_dbs(url, &DbType::Mysql).await.unwrap(),
            ),

            ExtractorConfig::PgBasic { url, .. } => (
                url.to_string(),
                ExtractorUtil::list_dbs(url, &DbType::Pg).await.unwrap(),
            ),

            _ => (String::new(), vec![]),
        };

        let dst_url = match &config.sinker {
            SinkerConfig::MysqlBasic { url, .. } => url.to_string(),
            SinkerConfig::PgBasic { url, .. } => url.to_string(),
            _ => String::new(),
        };

        (src_url, dst_url, log_level, dbs, filter, buffer, shut_down)
    }

    fn compare_models(
        &self,
        src_models: &HashMap<String, StructModel>,
        dst_models: &HashMap<String, StructModel>,
    ) {
        assert_eq!(src_models.len(), dst_models.len());

        for (key, src_model) in src_models.iter() {
            let dst_model = dst_models.get(key).unwrap();

            println!(
                "comparing models, src_model: {:?}, dst_model: {:?}",
                src_model, dst_model
            );
            assert_eq!(src_model, dst_model);
        }
    }
}
