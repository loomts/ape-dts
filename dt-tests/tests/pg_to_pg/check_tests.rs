#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn check_basic_test() {
        TestBase::run_check_test("pg_to_pg/check/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_route_test() {
        TestBase::run_check_test("pg_to_pg/check/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_sample_test() {
        TestBase::run_check_test("pg_to_pg/check/sample_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_struct_basic_test() {
        TestBase::run_check_test("pg_to_pg/check/basic_struct_test").await;
    }
}
