#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn revise_basic_test() {
        TestBase::run_revise_test("mysql_to_mysql/revise/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn revise_route_test() {
        TestBase::run_revise_test("mysql_to_mysql/revise/route_test").await;
    }
}
