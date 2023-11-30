#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn revise_basic_test() {
        TestBase::run_revise_test("pg_to_pg/revise/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn revise_route_test() {
        TestBase::run_revise_test("pg_to_pg/revise/route_test").await;
    }
}
