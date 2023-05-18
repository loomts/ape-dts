#[cfg(test)]
pub mod tests_utils {
    use dt_common::{
        config::{
            config_enums::DbType, extractor_config::ExtractorConfig, sinker_config::SinkerConfig,
            task_config::TaskConfig,
        },
        utils::database_mock::DatabaseMockUtils,
    };

    use crate::config::task_config::PrecheckTaskConfig;

    pub const PRECHECK_IT_PATH: &str = "/dt-precheck/src/test/scripts";

    pub fn get_source_info(extractor_config: &ExtractorConfig) -> (Option<String>, Option<DbType>) {
        let mut result_url = String::from("");
        let mut result_type: Option<DbType> = None;

        match extractor_config {
            ExtractorConfig::MysqlBasic { url, .. } => {
                result_url = url.clone();
                result_type = Some(DbType::Mysql);
            }

            ExtractorConfig::PgBasic { url, .. } => {
                result_url = url.clone();
                result_type = Some(DbType::Pg);
            }

            _ => {}
        }

        (Some(result_url), result_type)
    }

    pub fn get_sink_info(sinker_config: &SinkerConfig) -> (Option<String>, Option<DbType>) {
        let mut result_url = String::from("");
        let mut result_type: Option<DbType> = None;

        match sinker_config {
            SinkerConfig::MysqlBasic { url, .. } => {
                result_url = url.clone();
                result_type = Some(DbType::Mysql);
            }

            SinkerConfig::PgBasic { url, .. } => {
                result_url = url.clone();
                result_type = Some(DbType::Pg);
            }

            _ => {}
        }

        (Some(result_url), result_type)
    }

    pub async fn precheck_mock_handler(factory: &DatabaseMockUtils, script_relative_path: &str) {
        _ = factory
            .load_data(format!("{}{}", PRECHECK_IT_PATH.to_string(), script_relative_path).as_str())
            .await;
    }

    pub async fn init_config_for_test(
        env_path: &str,
        config: &str,
    ) -> (
        TaskConfig,
        PrecheckTaskConfig,
        DatabaseMockUtils,
        DatabaseMockUtils,
    ) {
        let task_config = TaskConfig::new_with_envs(config, Some(env_path.to_string()));
        let precheck_config = PrecheckTaskConfig::new(&config);
        // build database instance
        let (source_url, source_db_type): (Option<String>, Option<DbType>) =
            get_source_info(&task_config.extractor);
        println!("source_url:{}", source_url.clone().unwrap());
        let source_db_mock_factory =
            DatabaseMockUtils::new(source_url.unwrap(), source_db_type.unwrap(), 2, 10)
                .await
                .unwrap();
        let (sink_url, sink_db_type): (Option<String>, Option<DbType>) =
            get_sink_info(&task_config.sinker);
        println!("sink_url:{}", sink_url.clone().unwrap());
        let sink_db_mock_factory: DatabaseMockUtils =
            DatabaseMockUtils::new(sink_url.unwrap(), sink_db_type.unwrap(), 2, 10)
                .await
                .unwrap();
        (
            task_config,
            precheck_config,
            source_db_mock_factory,
            sink_db_mock_factory,
        )
    }
}
