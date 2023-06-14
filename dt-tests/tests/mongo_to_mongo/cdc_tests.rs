#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/basic_test", 3000, 10000).await;
    }
}
