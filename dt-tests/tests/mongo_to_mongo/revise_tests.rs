#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn revise_basic_test() {
        TestBase::run_mongo_revise_test("mongo_to_mongo/revise/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn revise_route_test() {
        TestBase::run_mongo_revise_test("mongo_to_mongo/revise/route_test").await;
    }
}
