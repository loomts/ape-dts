#[cfg(test)]
mod test {
    use crate::test::test_runner::TestRunner;
    use futures::executor::block_on;
    use serial_test::serial;
    use tokio::runtime::Runtime;
    const TEST_DIR: &str = "src/test/pg_to_pg";

    #[test]
    #[serial]
    fn cdc_basic_test() {
        let src_ddl_file = format!("{}/cdc_basic_test/src_ddl.sql", TEST_DIR);
        let dst_ddl_file = format!("{}/cdc_basic_test/dst_ddl.sql", TEST_DIR);
        let src_dml_file = format!("{}/cdc_basic_test/src_dml.sql", TEST_DIR);
        let task_config_file = format!("{}/cdc_basic_test/task_config.ini", TEST_DIR);

        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(TestRunner::new(&task_config_file)).unwrap();
        rt.block_on(runner.run_cdc_test(
            &src_ddl_file,
            &dst_ddl_file,
            &src_dml_file,
            &task_config_file,
        ))
        .unwrap();
    }
}
