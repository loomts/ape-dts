#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_do_nothing_test() {
        TestBase::run_snapshot_test("mysql_to_mysql_lua/snapshot/do_nothing_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_snapshot_lua_test("mysql_to_mysql_lua/snapshot/basic_test").await;
    }
}
