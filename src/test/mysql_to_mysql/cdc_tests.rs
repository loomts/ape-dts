#[cfg(test)]
mod test {
    use crate::test::test_runner::TestRunner;
    use futures::executor::block_on;
    use serial_test::serial;

    const TEST_DIR: &str = "src/test/mysql_to_mysql";

    #[test]
    #[serial]
    fn cdc_basic_test() {
        let src_ddl_file = format!("{}/cdc_basic_test/src_ddl.sql", TEST_DIR);
        let dst_ddl_file = format!("{}/cdc_basic_test/dst_ddl.sql", TEST_DIR);
        let src_dml_file = format!("{}/cdc_basic_test/src_dml.sql", TEST_DIR);
        let task_config_file = format!("{}/cdc_basic_test/task_config.ini", TEST_DIR);

        let runner = block_on(TestRunner::new(&task_config_file)).unwrap();
        block_on(runner.run_cdc_test(
            &src_ddl_file,
            &dst_ddl_file,
            &src_dml_file,
            &task_config_file,
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

        let runner = block_on(TestRunner::new(&env_file)).unwrap();
        block_on(runner.run_cdc_test(
            &src_ddl_file,
            &dst_ddl_file,
            &src_dml_file,
            &task_config_file,
        ))
        .unwrap();
    }
}
