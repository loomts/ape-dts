#[cfg(test)]
mod test {
    use crate::test::test_base::TestBase;
    use serial_test::serial;

    #[test]
    #[serial]
    fn cdc_basic_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc_basic_test", 7000, 5000);
    }
}
