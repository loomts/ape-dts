#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use futures::executor::block_on;
    use serial_test::serial;

    use crate::{
        error::Error,
        task::{task_runner::TaskRunner, task_util::TaskUtil},
        test::test_runner::TestRunner,
    };

    const CDC_TASK_START_MILLIS: u64 = 3000;
    const BINLOG_PARSE_MILLIS: u64 = 1000;
    const TEST_DIR: &str = "src/test/mysql_to_mysql";

    #[test]
    #[serial]
    fn cdc_basic_test() {
        let env_file = format!("{}/.env", TEST_DIR);
        let src_ddl_file = format!("{}/cdc_basic_test/src_ddl.sql", TEST_DIR);
        let dst_ddl_file = format!("{}/cdc_basic_test/dst_ddl.sql", TEST_DIR);
        let src_dml_file = format!("{}/cdc_basic_test/src_dml.sql", TEST_DIR);
        let task_config_file = format!("{}/cdc_basic_test/task_config.ini", TEST_DIR);

        // compare src and dst data
        let cols = TestRunner::get_default_tb_cols();
        let src_tbs = TestRunner::get_default_tbs();
        let dst_tbs = TestRunner::get_default_tbs();

        let runner = block_on(TestRunner::new(&env_file)).unwrap();
        block_on(run_cdc_test(
            &runner,
            &src_ddl_file,
            &dst_ddl_file,
            &src_dml_file,
            &task_config_file,
            &src_tbs,
            &dst_tbs,
            &cols,
        ))
        .unwrap();
    }

    #[test]
    #[serial]
    fn cdc_uk_changed_test() {
        let env_file = format!("{}/.env", TEST_DIR);
        let src_ddl_file = format!("{}/cdc_uk_changed_test/src_ddl.sql", TEST_DIR);
        let dst_ddl_file = format!("{}/cdc_uk_changed_test/dst_ddl.sql", TEST_DIR);
        let src_dml_file = format!("{}/cdc_uk_changed_test/src_dml.sql", TEST_DIR);
        let task_config_file = format!("{}/cdc_uk_changed_test/task_config.ini", TEST_DIR);

        // compare src and dst data
        let cols = TestRunner::get_default_tb_cols();
        let src_tbs = vec!["test_db_1.one_pk_multi_uk"];
        let dst_tbs = vec!["test_db_1.one_pk_multi_uk"];

        let runner = block_on(TestRunner::new(&env_file)).unwrap();
        block_on(run_cdc_test(
            &runner,
            &src_ddl_file,
            &dst_ddl_file,
            &src_dml_file,
            &task_config_file,
            &src_tbs,
            &dst_tbs,
            &cols,
        ))
        .unwrap();
    }

    async fn run_cdc_test(
        runner: &TestRunner,
        src_ddl_file: &str,
        dst_ddl_file: &str,
        src_dml_file: &str,
        task_config_file: &str,
        src_tbs: &Vec<&str>,
        dst_tbs: &Vec<&str>,
        cols: &Vec<&str>,
    ) -> Result<(), Error> {
        // prepare src and dst tables
        runner.prepare_test_tbs(src_ddl_file, dst_ddl_file).await?;

        // start task
        let config_file = task_config_file.to_string();
        thread::spawn(move || {
            block_on(TaskRunner::start_task(&config_file)).unwrap();
        });

        TaskUtil::sleep_millis(CDC_TASK_START_MILLIS).await;

        // load dml sqls
        let src_dml_sqls = runner.load_sqls(src_dml_file).await?;
        let mut src_insert_sqls = Vec::new();
        let mut src_update_sqls = Vec::new();
        let mut src_delete_sqls = Vec::new();

        for mut sql in src_dml_sqls {
            sql = sql.to_lowercase();
            if sql.starts_with("insert") {
                src_insert_sqls.push(sql);
            } else if sql.starts_with("update") {
                src_update_sqls.push(sql);
            } else {
                src_delete_sqls.push(sql);
            }
        }

        // insert src data
        runner
            .execute_sqls(&src_insert_sqls, &runner.src_conn_pool)
            .await?;
        thread::sleep(Duration::from_millis(BINLOG_PARSE_MILLIS));
        assert!(
            runner
                .compare_data_for_tbs(&src_tbs, &dst_tbs, &cols)
                .await?
        );

        // update src data
        runner
            .execute_sqls(&src_update_sqls, &runner.src_conn_pool)
            .await?;
        TaskUtil::sleep_millis(BINLOG_PARSE_MILLIS).await;
        assert!(
            runner
                .compare_data_for_tbs(&src_tbs, &dst_tbs, &cols)
                .await?
        );

        // delete src data
        runner
            .execute_sqls(&src_delete_sqls, &runner.src_conn_pool)
            .await?;
        TaskUtil::sleep_millis(BINLOG_PARSE_MILLIS).await;
        assert!(
            runner
                .compare_data_for_tbs(&src_tbs, &dst_tbs, &cols)
                .await?
        );

        Ok(())
    }
}
