#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_rdb_redis_snapshot_test("mysql_to_redis/snapshot/basic_test").await;
    }
}
