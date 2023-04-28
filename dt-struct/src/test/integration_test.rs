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
            extractor_config: task_config.extractor.clone(),
            sinker_config: task_config.sinker.clone(),
            filter_config: task_config.filter.clone(),
            router_config: task_config.router.clone(),
            struct_config: struct_config.struct_config.clone(),
        };

        let mut struct_config_check = struct_config.struct_config.clone();
        struct_config_check.is_do_check = true;
        let check_builder = StructBuilder {
            extractor_config: task_config.extractor.clone(),
            sinker_config: task_config.sinker.clone(),
            filter_config: task_config.filter.clone(),
            router_config: task_config.router.clone(),
            struct_config: struct_config_check,
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

        _ = builder.build_job().await;
        assert!(check_builder.build_job().await.is_ok());

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

    #[tokio::test]
    async fn pg2pg_basic_test() {
        let mysql2mysql_env: String = format!(
            "{}{}/pg2pg/.env",
            WorkDirUtil::get_project_root().unwrap(),
            STRUCT_IT_PATH
        );
        let config = WorkDirUtil::get_absolute_by_relative(
            format!(
                "{}/pg2pg/basic_test/task_config.ini",
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
            extractor_config: task_config.extractor.clone(),
            sinker_config: task_config.sinker.clone(),
            filter_config: task_config.filter.clone(),
            router_config: task_config.router.clone(),
            struct_config: struct_config.struct_config.clone(),
        };

        let mut struct_config_check = struct_config.struct_config.clone();
        struct_config_check.is_do_check = true;
        let check_builder = StructBuilder {
            extractor_config: task_config.extractor.clone(),
            sinker_config: task_config.sinker.clone(),
            filter_config: task_config.filter.clone(),
            router_config: task_config.router.clone(),
            struct_config: struct_config_check,
        };

        // clean data and init test data
        struct_mock_handler(
            &source_db_mock_factory,
            vec![
                "/pg2pg/basic_test/source_clean.sql",
                "/pg2pg/basic_test/source_ddl.sql",
            ],
        )
        .await;
        struct_mock_handler(
            &sink_db_mock_factory,
            vec![
                "/pg2pg/basic_test/sink_clean.sql",
                "/pg2pg/basic_test/sink_ddl.sql",
            ],
        )
        .await;

        _ = builder.build_job().await;
        assert!(check_builder.build_job().await.is_ok());

        // clean and release pool
        struct_mock_handler(
            &source_db_mock_factory,
            vec!["/pg2pg/basic_test/source_clean.sql"],
        )
        .await;
        struct_mock_handler(
            &sink_db_mock_factory,
            vec!["/pg2pg/basic_test/sink_clean.sql"],
        )
        .await;

        source_db_mock_factory.release_pool().await;
        sink_db_mock_factory.release_pool().await;
    }
}
