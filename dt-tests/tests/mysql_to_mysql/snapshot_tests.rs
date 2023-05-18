#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_on_duplicate_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/on_duplicate_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_wildchar_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/wildchar_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_charset_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/charset_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_timezone_test() {
        println!("snapshot_timezone_test can be covered by test: cdc_basic_test, table: one_pk_no_uk, field: f_13 timestamp(6), the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00 ")
    }
}
