#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::tests::test_base::TestBase;

    #[test]
    #[serial]
    fn revise_basic_test() {
        TestBase::run_revise_test("pg_to_pg/revise_basic_test");
    }
}
