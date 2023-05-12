#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::tests::test_base::TestBase;

    #[test]
    #[serial]
    fn review_basic_test() {
        TestBase::run_review_test("pg_to_pg/review_basic_test");
    }
}
