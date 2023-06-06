#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn check_basic_test() {
        TestBase::run_check_test("mysql_to_mysql/check/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_all_cols_pk_test() {
        TestBase::run_check_test("mysql_to_mysql/check/all_cols_pk_test").await;
    }

    // this should run seperately from other tests since it has a different check log dir,
    // all tests will be run in one progress, the log4rs will only be initialized once, it makes this test fails
    // #[tokio::test]
    // #[serial]
    // async fn check_set_check_log_dir_test() {
    //     TestBase::run_check_test("mysql_to_mysql/check_set_check_log_dir_test").await;
    // }
}
