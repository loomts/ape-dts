#[cfg(test)]
mod test {
    use crate::test_runner::test_base::TestBase;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn cdc_cmds_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/5_0/cmds_test", 2000, 10000).await;
    }
}
