#[cfg(test)]
mod test {
    use crate::test::test_base::TestBase;
    use serial_test::serial;

    #[test]
    #[serial]
    fn check_basic_test() {
        TestBase::run_check_test("pg_to_pg/check_basic_test");
    }
}
