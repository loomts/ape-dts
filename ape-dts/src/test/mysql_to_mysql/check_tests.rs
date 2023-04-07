#[cfg(test)]
mod test {
    use crate::test::test_base::TestBase;
    use serial_test::serial;

    #[test]
    #[serial]
    fn check_basic_test() {
        TestBase::run_check_test("mysql_to_mysql/check_basic_test");
    }

    #[test]
    #[serial]
    fn check_all_cols_pk_test() {
        TestBase::run_check_test("mysql_to_mysql/check_all_cols_pk_test");
    }

    // this should run seperately from other tests since it has a different check log dir,
    // all tests will be run in one progress, the log4rs will only be initialized once, it makes this test fails
    // #[test]
    // #[serial]
    // fn check_set_check_log_dir_test() {
    //     TestBase::run_check_test("mysql_to_mysql/check_set_check_log_dir_test");
    // }
}
