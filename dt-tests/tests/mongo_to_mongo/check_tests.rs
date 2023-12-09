#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn check_basic_test() {
        TestBase::run_mongo_check_test("mongo_to_mongo/check/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_route_test() {
        TestBase::run_mongo_check_test("mongo_to_mongo/check/route_test").await;
    }
}
