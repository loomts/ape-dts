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

    #[test]
    #[serial]
    fn cdc_timezone_test() {
        println!("cdc_timezone_test can be covered by test: cdc_basic_test, table: one_pk_no_uk, field: f_13 timestamp(6), the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00 ")
    }
}
