#[cfg(test)]
mod test {
    use crate::test::test_runner::TestRunner;
    use serial_test::serial;
    use tokio::runtime::Runtime;

    #[test]
    #[serial]
    fn check_basic_test() {
        let rt = Runtime::new().unwrap();
        let runner = rt
            .block_on(TestRunner::new("pg_to_pg/check_basic_test"))
            .unwrap();
        rt.block_on(runner.run_check_test()).unwrap();
    }
}
