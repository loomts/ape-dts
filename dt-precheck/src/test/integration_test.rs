#[cfg(test)]
mod tests {
    use dt_common::{
        config::task_config::TaskConfig,
        utils::{database_mock::DatabaseMockUtils, work_dir_util::WorkDirUtil},
    };

    use crate::{
        config::task_config::PrecheckTaskConfig,
        connector::checker_connector::CheckerConnector,
        meta::check_item::CheckItem,
        test::utils::tests_utils::{init_config_for_test, precheck_mock_handler, PRECHECK_IT_PATH},
    };

    #[tokio::test]
    async fn mysql2mysql_struct_exists_test() {
        let mysql2mysql_env: String = format!(
            "{}{}/mysql2mysql/.env",
            WorkDirUtil::get_project_root().unwrap(),
            PRECHECK_IT_PATH
        );
        let config = WorkDirUtil::get_absolute_by_relative(
            format!(
                "{}/mysql2mysql/check_struct_existed/task_config.ini",
                PRECHECK_IT_PATH.to_string()
            )
            .as_str(),
        )
        .unwrap();

        let (task_config, precheck_config, source_db_mock_factory, sink_db_mock_factory): (
            TaskConfig,
            PrecheckTaskConfig,
            DatabaseMockUtils,
            DatabaseMockUtils,
        ) = init_config_for_test(&mysql2mysql_env, &config).await;

        // clean data
        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_existed/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_existed/sink_clean.sql",
        )
        .await;

        let checker_connector =
            CheckerConnector::build(precheck_config.precheck.clone(), task_config.clone());

        // check before load ddl, schema
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfStructExisted.to_string() {
                assert!(!result_some.is_validate);
            } else {
                assert!(result_some.is_validate);
            }
        }

        // install source struct and sink database
        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_existed/source_ddl.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_existed/sink_ddl_1.sql",
        )
        .await;
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok() && result.unwrap().is_validate);
        }

        // check if not set do struct
        let mut precheck_config_without_struct = precheck_config.precheck.clone();
        precheck_config_without_struct.do_struct_init = false;
        let checker_connector =
            CheckerConnector::build(precheck_config_without_struct, task_config.clone());
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfStructExisted.to_string()
                && !result_some.is_source
            {
                assert!(!result_some.is_validate);
            } else {
                assert!(result_some.is_validate);
            }
        }

        // install sink struct
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_existed/sink_ddl_2.sql",
        )
        .await;
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok() && result.unwrap().is_validate);
        }

        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_existed/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_existed/sink_clean.sql",
        )
        .await;

        source_db_mock_factory.release_pool().await;
        sink_db_mock_factory.release_pool().await;
    }

    #[tokio::test]
    async fn mysql2mysql_struct_supported_test() {
        let mysql2mysql_env: String = format!(
            "{}{}/mysql2mysql/.env",
            WorkDirUtil::get_project_root().unwrap(),
            PRECHECK_IT_PATH
        );
        let config = WorkDirUtil::get_absolute_by_relative(
            format!(
                "{}/mysql2mysql/check_struct_supported/task_config.ini",
                PRECHECK_IT_PATH.to_string()
            )
            .as_str(),
        )
        .unwrap();

        let (task_config, mut precheck_config, source_db_mock_factory, sink_db_mock_factory): (
            TaskConfig,
            PrecheckTaskConfig,
            DatabaseMockUtils,
            DatabaseMockUtils,
        ) = init_config_for_test(&mysql2mysql_env, &config).await;

        // clean data
        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_supported/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_supported/sink_clean.sql",
        )
        .await;

        let checker_connector =
            CheckerConnector::build(precheck_config.precheck.clone(), task_config.clone());

        // check before load ddl, schema, no database
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfTableStructSupported.to_string() {
                assert!(result_some.is_validate);
            }
        }

        // install source struct and sink database
        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_supported/source_ddl.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_supported/sink_ddl.sql",
        )
        .await;
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok() && result.unwrap().is_validate);
        }

        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_supported/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_supported/sink_clean.sql",
        )
        .await;

        // check if set 'no init-struct' with empty struct
        precheck_config.precheck.do_struct_init = false;
        let checker_connector =
            CheckerConnector::build(precheck_config.precheck.clone(), task_config.clone());
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfTableStructSupported.to_string() {
                assert!(result_some.is_validate);
            }
        }

        source_db_mock_factory.release_pool().await;
        sink_db_mock_factory.release_pool().await;
    }

    #[tokio::test]
    async fn mysql2mysql_struct_supported_test_no_pk() {
        let mysql2mysql_env: String = format!(
            "{}{}/mysql2mysql/.env",
            WorkDirUtil::get_project_root().unwrap(),
            PRECHECK_IT_PATH
        );
        let config = WorkDirUtil::get_absolute_by_relative(
            format!(
                "{}/mysql2mysql/check_struct_supported/no_pk/task_config.ini",
                PRECHECK_IT_PATH.to_string()
            )
            .as_str(),
        )
        .unwrap();

        let (task_config, precheck_config, source_db_mock_factory, sink_db_mock_factory): (
            TaskConfig,
            PrecheckTaskConfig,
            DatabaseMockUtils,
            DatabaseMockUtils,
        ) = init_config_for_test(&mysql2mysql_env, &config).await;

        // clean data
        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_supported/no_pk/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_supported/no_pk/sink_clean.sql",
        )
        .await;

        let checker_connector =
            CheckerConnector::build(precheck_config.precheck.clone(), task_config.clone());

        // install source struct and sink database
        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_supported/no_pk/source_ddl.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_supported/no_pk/sink_ddl.sql",
        )
        .await;
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfTableStructSupported.to_string() {
                assert!(!result_some.is_validate);
            } else {
                assert!(result_some.is_validate);
            }
        }

        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_supported/no_pk/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_supported/no_pk/sink_clean.sql",
        )
        .await;

        source_db_mock_factory.release_pool().await;
        sink_db_mock_factory.release_pool().await;
    }

    #[tokio::test]
    async fn mysql2mysql_struct_supported_test_have_fk() {
        let mysql2mysql_env: String = format!(
            "{}{}/mysql2mysql/.env",
            WorkDirUtil::get_project_root().unwrap(),
            PRECHECK_IT_PATH
        );
        let config = WorkDirUtil::get_absolute_by_relative(
            format!(
                "{}/mysql2mysql/check_struct_supported/have_fk/task_config.ini",
                PRECHECK_IT_PATH.to_string()
            )
            .as_str(),
        )
        .unwrap();

        let (task_config, precheck_config, source_db_mock_factory, sink_db_mock_factory): (
            TaskConfig,
            PrecheckTaskConfig,
            DatabaseMockUtils,
            DatabaseMockUtils,
        ) = init_config_for_test(&mysql2mysql_env, &config).await;

        // clean data
        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_supported/have_fk/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_supported/have_fk/sink_clean.sql",
        )
        .await;

        let checker_connector =
            CheckerConnector::build(precheck_config.precheck.clone(), task_config.clone());

        // install source struct and sink database
        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_supported/have_fk/source_ddl.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_supported/have_fk/sink_ddl.sql",
        )
        .await;
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfTableStructSupported.to_string() {
                assert!(!result_some.is_validate);
            } else {
                assert!(result_some.is_validate);
            }
        }

        precheck_mock_handler(
            &source_db_mock_factory,
            "/mysql2mysql/check_struct_supported/have_fk/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/mysql2mysql/check_struct_supported/have_fk/sink_clean.sql",
        )
        .await;

        source_db_mock_factory.release_pool().await;
        sink_db_mock_factory.release_pool().await;
    }

    #[tokio::test]
    async fn pg2pg_struct_exists_test() {
        let pg2pg_env: String = format!(
            "{}{}/pg2pg/.env",
            WorkDirUtil::get_project_root().unwrap(),
            PRECHECK_IT_PATH
        );
        let config = WorkDirUtil::get_absolute_by_relative(
            format!(
                "{}/pg2pg/check_struct_existed/task_config.ini",
                PRECHECK_IT_PATH.to_string()
            )
            .as_str(),
        )
        .unwrap();

        let (task_config, precheck_config, source_db_mock_factory, sink_db_mock_factory): (
            TaskConfig,
            PrecheckTaskConfig,
            DatabaseMockUtils,
            DatabaseMockUtils,
        ) = init_config_for_test(&pg2pg_env, &config).await;

        // clean data
        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_existed/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_existed/sink_clean.sql",
        )
        .await;

        let checker_connector =
            CheckerConnector::build(precheck_config.precheck.clone(), task_config.clone());

        // check before load ddl, schema
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfStructExisted.to_string() {
                assert!(!result_some.is_validate);
            } else {
                assert!(result_some.is_validate);
            }
        }

        // install source struct and sink database
        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_existed/source_ddl.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_existed/sink_ddl_1.sql",
        )
        .await;
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok() && result.unwrap().is_validate);
        }

        // check if not set do struct
        let mut precheck_config_without_struct = precheck_config.precheck.clone();
        precheck_config_without_struct.do_struct_init = false;
        let checker_connector =
            CheckerConnector::build(precheck_config_without_struct, task_config.clone());
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfStructExisted.to_string()
                && !result_some.is_source
            {
                assert!(!result_some.is_validate);
            } else {
                assert!(result_some.is_validate);
            }
        }

        // install sink struct
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_existed/sink_ddl_2.sql",
        )
        .await;
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok() && result.unwrap().is_validate);
        }

        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_existed/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_existed/sink_clean.sql",
        )
        .await;

        source_db_mock_factory.release_pool().await;
        sink_db_mock_factory.release_pool().await;
    }

    #[tokio::test]
    async fn pg2pg_struct_supported_test() {
        let pg2pg_env: String = format!(
            "{}{}/pg2pg/.env",
            WorkDirUtil::get_project_root().unwrap(),
            PRECHECK_IT_PATH
        );
        let config = WorkDirUtil::get_absolute_by_relative(
            format!(
                "{}/pg2pg/check_struct_supported/task_config.ini",
                PRECHECK_IT_PATH.to_string()
            )
            .as_str(),
        )
        .unwrap();

        let (task_config, mut precheck_config, source_db_mock_factory, sink_db_mock_factory): (
            TaskConfig,
            PrecheckTaskConfig,
            DatabaseMockUtils,
            DatabaseMockUtils,
        ) = init_config_for_test(&pg2pg_env, &config).await;

        // clean data
        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_supported/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_supported/sink_clean.sql",
        )
        .await;

        let checker_connector =
            CheckerConnector::build(precheck_config.precheck.clone(), task_config.clone());

        // check before load ddl, schema, no database
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfTableStructSupported.to_string() {
                assert!(result_some.is_validate);
            }
        }

        // install source struct and sink database
        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_supported/source_ddl.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_supported/sink_ddl.sql",
        )
        .await;
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok() && result.unwrap().is_validate);
        }

        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_supported/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_supported/sink_clean.sql",
        )
        .await;

        // check if set 'no init-struct' with empty struct
        precheck_config.precheck.do_struct_init = false;
        let checker_connector =
            CheckerConnector::build(precheck_config.precheck.clone(), task_config.clone());
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfTableStructSupported.to_string() {
                assert!(result_some.is_validate);
            }
        }

        source_db_mock_factory.release_pool().await;
        sink_db_mock_factory.release_pool().await;
    }

    #[tokio::test]
    async fn pg2pg_struct_supported_test_have_fk() {
        let pg2pg_env: String = format!(
            "{}{}/pg2pg/.env",
            WorkDirUtil::get_project_root().unwrap(),
            PRECHECK_IT_PATH
        );
        let config = WorkDirUtil::get_absolute_by_relative(
            format!(
                "{}/pg2pg/check_struct_supported/have_fk/task_config.ini",
                PRECHECK_IT_PATH.to_string()
            )
            .as_str(),
        )
        .unwrap();

        let (task_config, precheck_config, source_db_mock_factory, sink_db_mock_factory): (
            TaskConfig,
            PrecheckTaskConfig,
            DatabaseMockUtils,
            DatabaseMockUtils,
        ) = init_config_for_test(&pg2pg_env, &config).await;

        // clean data
        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_supported/have_fk/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_supported/have_fk/sink_clean.sql",
        )
        .await;

        let checker_connector =
            CheckerConnector::build(precheck_config.precheck.clone(), task_config.clone());

        // install source struct and sink database
        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_supported/have_fk/source_ddl.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_supported/have_fk/sink_ddl.sql",
        )
        .await;
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfTableStructSupported.to_string() {
                assert!(!result_some.is_validate);
            } else {
                assert!(result_some.is_validate);
            }
        }

        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_supported/have_fk/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_supported/have_fk/sink_clean.sql",
        )
        .await;

        source_db_mock_factory.release_pool().await;
        sink_db_mock_factory.release_pool().await;
    }

    #[tokio::test]
    async fn pg2pg_struct_supported_test_no_pk() {
        let pg2pg_env: String = format!(
            "{}{}/pg2pg/.env",
            WorkDirUtil::get_project_root().unwrap(),
            PRECHECK_IT_PATH
        );
        let config = WorkDirUtil::get_absolute_by_relative(
            format!(
                "{}/pg2pg/check_struct_supported/no_pk/task_config.ini",
                PRECHECK_IT_PATH.to_string()
            )
            .as_str(),
        )
        .unwrap();

        let (task_config, precheck_config, source_db_mock_factory, sink_db_mock_factory): (
            TaskConfig,
            PrecheckTaskConfig,
            DatabaseMockUtils,
            DatabaseMockUtils,
        ) = init_config_for_test(&pg2pg_env, &config).await;

        // clean data
        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_supported/no_pk/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_supported/no_pk/sink_clean.sql",
        )
        .await;

        let checker_connector =
            CheckerConnector::build(precheck_config.precheck.clone(), task_config.clone());

        // install source struct and sink database
        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_supported/no_pk/source_ddl.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_supported/no_pk/sink_ddl.sql",
        )
        .await;
        let results = checker_connector.check().await;
        assert!(results.is_ok());
        for result in results.unwrap() {
            assert!(result.is_ok());
            let result_some = result.unwrap();
            if result_some.check_type_name == CheckItem::CheckIfTableStructSupported.to_string() {
                assert!(!result_some.is_validate);
            } else {
                assert!(result_some.is_validate);
            }
        }

        precheck_mock_handler(
            &source_db_mock_factory,
            "/pg2pg/check_struct_supported/no_pk/source_clean.sql",
        )
        .await;
        precheck_mock_handler(
            &sink_db_mock_factory,
            "/pg2pg/check_struct_supported/no_pk/sink_clean.sql",
        )
        .await;

        source_db_mock_factory.release_pool().await;
        sink_db_mock_factory.release_pool().await;
    }
}
