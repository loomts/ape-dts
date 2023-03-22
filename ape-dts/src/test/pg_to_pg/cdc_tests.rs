#[cfg(test)]
mod test {
    use crate::test::{test_config_util::TestConfigUtil, test_runner::TestRunner};
    use serial_test::serial;
    use tokio::runtime::Runtime;

    #[test]
    #[serial]
    fn cdc_basic_test() {
        let rt = Runtime::new().unwrap();
        let runner = rt
            .block_on(TestRunner::new("pg_to_pg/cdc_basic_test"))
            .unwrap();
        let configs = TestConfigUtil::get_default_configs();
        rt.block_on(runner.run_cdc_test_with_different_configs(5000, 5000, &configs))
            .unwrap();
    }
}
