#[cfg(test)]
mod test {
    use crate::test_runner::test_base::TestBase;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn snapshot_cmds_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/8_0/cmds_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_json_test() {
        TestBase::run_redis_rejson_snapshot_test("redis_to_redis/snapshot/8_0/json_test").await;
    }
}
