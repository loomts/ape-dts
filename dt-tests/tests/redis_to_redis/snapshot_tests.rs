#[cfg(test)]
mod test {
    use crate::test_runner::test_base::TestBase;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/basic_test").await;
    }
}
