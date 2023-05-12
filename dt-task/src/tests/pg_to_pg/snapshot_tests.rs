#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::tests::test_base::TestBase;

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

    #[test]
    #[serial]
    fn snapshot_timezone_test() {
        println!("snapshot_timezone_test can be covered by test: snapshot_basic_test, table: timezone_table, the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00")
    }
}
