#[cfg(test)]
mod tests {
    use dt_common::{
        config::task_config::TaskConfig,
        utils::{database_mock::DatabaseMockUtils, work_dir_util::WorkDirUtil},
    };

    use crate::{
        config::task_config::StructTaskConfig,
        factory::database_worker_builder::StructBuilder,
        test::utils::tests_utils::{init_config_for_test, struct_mock_handler, STRUCT_IT_PATH},
    };

    #[tokio::test]
    async fn mysql2mysql_basic_test() {
        let mysql2mysql_env: String = format!(
            "{}{}/mysql2mysql/.env",
            WorkDirUtil::get_project_root().unwrap(),
            STRUCT_IT_PATH
        );
        let config = WorkDirUtil::get_absolute_by_relative(
            format!(
                "{}/mysql2mysql/basic_test/task_config.ini",
                STRUCT_IT_PATH.to_string()
            )
            .as_str(),
        )
        .unwrap();

        let (task_config, struct_config, source_db_mock_factory, sink_db_mock_factory): (
            TaskConfig,
            StructTaskConfig,
            DatabaseMockUtils,
            DatabaseMockUtils,
        ) = init_config_for_test(&mysql2mysql_env, &config).await;
        let builder = StructBuilder {
            extractor_config: task_config.extractor,
            sinker_config: task_config.sinker,
            filter_config: task_config.filter,
            router_config: task_config.router,
            struct_config: struct_config.struct_config,
        };

        // clean data and init test data
        struct_mock_handler(
            &source_db_mock_factory,
            vec![
                "/mysql2mysql/basic_test/source_clean.sql",
                "/mysql2mysql/basic_test/source_ddl.sql",
            ],
        )
        .await;
        struct_mock_handler(
            &sink_db_mock_factory,
            vec![
                "/mysql2mysql/basic_test/sink_clean.sql",
                "/mysql2mysql/basic_test/sink_ddl.sql",
            ],
        )
        .await;

        builder.build_job().await;

        // Todo: do check

        // clean and release pool
        struct_mock_handler(
            &source_db_mock_factory,
            vec!["/mysql2mysql/basic_test/source_clean.sql"],
        )
        .await;
        struct_mock_handler(
            &sink_db_mock_factory,
            vec!["/mysql2mysql/basic_test/sink_clean.sql"],
        )
        .await;

        source_db_mock_factory.release_pool().await;
        sink_db_mock_factory.release_pool().await;
    }
}
