#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn check_basic_test() {
        TestBase::run_check_test("mysql_to_mysql_case_sensitive/check/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_basic_struct_test() {
        TestBase::run_check_test("mysql_to_mysql_case_sensitive/check/basic_struct_test").await;
    }
}
