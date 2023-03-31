#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test::test_base::TestBase;

    #[test]
    #[serial]
    fn snapshot_basic_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot_basic_test");
    }

    #[test]
    #[serial]
    fn snapshot_on_duplicate_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot_on_duplicate_test");
    }

    #[test]
    #[serial]
    fn snapshot_wildchar_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot_wildchar_test");
    }

    #[test]
    #[serial]
    fn snapshot_charset_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot_charset_test");
    }
}
