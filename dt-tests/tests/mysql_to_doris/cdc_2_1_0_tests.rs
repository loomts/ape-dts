#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_cdc_test("mysql_to_doris/cdc/2_1_0/basic_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_json_test() {
        TestBase::run_cdc_test("mysql_to_doris/cdc/2_1_0/json_test", 3000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_json_to_string_test() {
        TestBase::run_cdc_test("mysql_to_doris/cdc/2_1_0/json_to_string_test", 3000, 5000).await;
    }
}
