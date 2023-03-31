#[cfg(test)]
mod test {
    use crate::test::test_base::TestBase;
    use serial_test::serial;

    #[test]
    #[serial]
    fn check_basic_test() {
        TestBase::run_check_test("mysql_to_mysql/check_basic_test");
    }
}
