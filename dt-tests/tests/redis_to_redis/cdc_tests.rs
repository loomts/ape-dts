#[cfg(test)]
mod test {
    use crate::test_runner::test_base::TestBase;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/basic_test", 2000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_multi_dbs_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/multi_dbs_test", 2000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_multi_exec_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/multi_exec_test", 2000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_cmds_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/cmds_test", 2000, 2000).await;
    }
}
