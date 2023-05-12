#[cfg(test)]
mod test {
    use crate::tests::test_base::TestBase;
    use serial_test::serial;

    #[test]
    #[serial]
    fn review_basic_test() {
        TestBase::run_review_test("mysql_to_mysql/review_basic_test");
    }
}
