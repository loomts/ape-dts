#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_cdc_test("mysql_to_starrocks/cdc/3_2_11/basic_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_json_test() {
        TestBase::run_cdc_test("mysql_to_starrocks/cdc/3_2_11/json_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_json_to_string_test() {
        TestBase::run_cdc_test(
            "mysql_to_starrocks/cdc/3_2_11/json_to_string_test",
            3000,
            3000,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_soft_delete_test() {
        TestBase::run_rdb_starrocks_cdc_test(
            "mysql_to_starrocks/cdc/3_2_11/soft_delete_test",
            3000,
            3000,
        )
        .await;
    }
}
