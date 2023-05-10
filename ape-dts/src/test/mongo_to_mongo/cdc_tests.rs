#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test::test_base::TestBase;

    #[test]
    #[serial]
    fn cdc_basic_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc_basic_test", 3000, 10000);
    }
}
