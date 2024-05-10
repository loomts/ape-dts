#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_do_nothing_test() {
        TestBase::run_cdc_test("pg_to_pg_lua/cdc/do_nothing_test", 3000, 7000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_cdc_lua_test("pg_to_pg_lua/cdc/basic_test", 3000, 7000).await;
    }
}
