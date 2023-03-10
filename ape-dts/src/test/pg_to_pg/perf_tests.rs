#[cfg(test)]
mod test {

    use crate::test::test_runner::TestRunner;
    use serial_test::serial;
    use tokio::runtime::Runtime;

    // #[test]
    #[serial]
    fn snapshot_perf_test() {
        let rt = Runtime::new().unwrap();
        let runner = rt
            .block_on(TestRunner::new("pg_to_pg/snapshot_perf_test"))
            .unwrap();
        rt.block_on(runner.run_perf_test(200)).unwrap();
    }
}
