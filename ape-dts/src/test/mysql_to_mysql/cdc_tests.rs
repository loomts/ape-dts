#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test::test_base::TestBase;

    #[test]
    #[serial]
    fn cdc_basic_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc_basic_test", 3000, 1000);
    }

    #[test]
    #[serial]
    fn cdc_uk_changed_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc_uk_changed_test", 3000, 1000);
    }

    #[test]
    #[serial]
    fn cdc_charset_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc_charset_test", 3000, 1000);
    }
}
