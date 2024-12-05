#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_rdb_starrocks_cdc_test("pg_to_starrocks/cdc/3_2_11/basic_test", 3000, 5000)
            .await;
    }
}
