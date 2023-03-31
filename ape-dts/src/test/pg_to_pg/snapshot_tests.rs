#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test::test_base::TestBase;

    #[test]
    #[serial]
    fn snapshot_basic_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot_basic_test");
    }

    /// dst table already has records with same primary keys of src table,
    /// src data should be synced to dst table by "ON CONFLICT (pk) DO UPDATE SET"
    #[test]
    #[serial]
    fn snapshot_on_duplicate_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot_on_duplicate_test");
    }

    #[test]
    #[serial]
    fn snapshot_wildchar_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot_wildchar_test");
    }

    #[test]
    #[serial]
    fn snapshot_postgis_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot_postgis_test");
    }

    #[test]
    #[serial]
    fn snapshot_postgis_array_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot_postgis_array_test");
    }

    #[test]
    #[serial]
    fn snapshot_charset_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot_charset_test");
    }

    #[test]
    #[serial]
    fn snapshot_charset_euc_cn_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot_charset_euc_cn_test");
    }
}
