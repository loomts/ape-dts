#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::tests::test_base::TestBase;

    #[test]
    #[serial]
    fn check_basic_test() {
        TestBase::run_check_test("pg_to_pg/check_basic_test");
    }
}
