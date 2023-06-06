#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/basic_test").await;
    }

    /// dst table already has records with same primary keys of src table,
    /// src data should be synced to dst table by "ON CONFLICT (pk) DO UPDATE SET"
    #[tokio::test]
    #[serial]
    async fn snapshot_on_duplicate_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/on_duplicate_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_wildchar_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/wildchar_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_postgis_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/postgis_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_postgis_array_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/postgis_array_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_charset_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/charset_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_charset_euc_cn_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/charset_euc_cn_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_timezone_test() {
        println!("snapshot_timezone_test can be covered by test: snapshot_basic_test, table: timezone_table, the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00")
    }
}
