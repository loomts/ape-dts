#[cfg(test)]
mod test {
    use crate::test_runner::test_base::TestBase;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn cdc_cmds_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/8_0/cmds_test", 2000, 10000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_json_test() {
        TestBase::run_redis_rejson_cdc_test("redis_to_redis/cdc/8_0/json_test", 2000, 10000).await;
    }
}
