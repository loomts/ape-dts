#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::tests::test_base::TestBase;

    #[test]
    #[serial]
    fn cdc_basic_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc_basic_test", 7000, 5000);
    }
}
