#[cfg(test)]
mod test {
    use serial_test::serial;
    use tokio::runtime::Runtime;

    use crate::test::{test_config_util::TestConfigUtil, test_runner::TestRunner};

    #[test]
    #[serial]
    fn cdc_basic_test() {
        let rt = Runtime::new().unwrap();
        let runner = rt
            .block_on(TestRunner::new("mysql_to_mysql/cdc_basic_test"))
            .unwrap();
        let configs = TestConfigUtil::get_default_configs();
        rt.block_on(runner.run_cdc_test_with_different_configs(3000, 1000, &configs))
            .unwrap();
    }

    #[test]
    #[serial]
    fn cdc_uk_changed_test() {
        let rt = Runtime::new().unwrap();
        let runner = rt
            .block_on(TestRunner::new("mysql_to_mysql/cdc_uk_changed_test"))
            .unwrap();
        let configs = TestConfigUtil::get_default_configs();
        rt.block_on(runner.run_cdc_test_with_different_configs(3000, 1000, &configs))
            .unwrap();
    }
}
