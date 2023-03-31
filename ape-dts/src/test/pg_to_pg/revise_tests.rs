#[cfg(test)]
mod test {
    use crate::test::test_base::TestBase;
    use serial_test::serial;

    #[test]
    #[serial]
    fn revise_basic_test() {
        TestBase::run_revise_test("pg_to_pg/revise_basic_test");
    }
}
