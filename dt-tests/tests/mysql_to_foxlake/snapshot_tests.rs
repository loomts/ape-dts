#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_snapshot_test("mysql_to_foxlake/snapshot/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_foxlake_test() {
        TestBase::run_snapshot_test("mysql_to_foxlake/snapshot/foxlake_types_test").await;
    }
}
