#[cfg(test)]
mod test {
    use crate::test::test_runner::TestRunner;
    use serial_test::serial;
    use tokio::runtime::Runtime;

    #[test]
    #[serial]
    fn snapshot_basic_test() {
        let rt = Runtime::new().unwrap();
        let runner = rt
            .block_on(TestRunner::new("pg_to_pg/snapshot_basic_test"))
            .unwrap();
        rt.block_on(runner.run_snapshot_test()).unwrap();
    }

    /// dst table already has records with same primary keys of src table,
    /// src data should be synced to dst table by "ON CONFLICT (pk) DO UPDATE SET"
    #[test]
    #[serial]
    fn snapshot_on_duplicate_test() {
        let rt = Runtime::new().unwrap();
        let runner = rt
            .block_on(TestRunner::new("pg_to_pg/snapshot_on_duplicate_test"))
            .unwrap();
        rt.block_on(runner.run_snapshot_test()).unwrap();
    }
}
