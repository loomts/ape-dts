#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test::test_base::TestBase;

    #[test]
    #[serial]
    fn revise_basic_test() {
        TestBase::run_revise_test("mysql_to_mysql/revise_basic_test");
    }
}
