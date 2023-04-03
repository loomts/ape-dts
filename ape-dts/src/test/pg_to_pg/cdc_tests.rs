#[cfg(test)]
mod test {
    use crate::test::test_base::TestBase;
    use serial_test::serial;

    #[test]
    #[serial]
    fn cdc_basic_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc_basic_test", 7000, 5000);
    }

    #[test]
    #[serial]
    fn cdc_postgis_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc_postgis_test", 7000, 5000);
    }

    #[test]
    #[serial]
    fn cdc_postgis_array_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc_postgis_array_test", 7000, 5000);
    }

    #[test]
    #[serial]
    fn cdc_charset_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc_charset_test", 7000, 5000);
    }

    #[test]
    #[serial]
    fn cdc_charset_euc_cn_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc_charset_euc_cn_test", 7000, 5000);
    }

    #[test]
    #[serial]
    fn cdc_timezone_test() {
        println!("cdc_timezone_test can be covered by test: cdc_basic_test, table: timezone_table, the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00")
    }
}
