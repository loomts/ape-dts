#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_snapshot_test("mysql_to_starrocks/snapshot/3_2_11/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_json_test() {
        TestBase::run_snapshot_test("mysql_to_starrocks/snapshot/3_2_11/json_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_json_to_string_test() {
        TestBase::run_snapshot_test("mysql_to_starrocks/snapshot/3_2_11/json_to_string_test").await;
    }
}
