#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_cdc_test("mysql_to_foxlake/cdc/basic_test", 3000, 40000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_foxlake_types_test() {
        TestBase::run_cdc_test("mysql_to_foxlake/cdc/foxlake_types_test", 3000, 50000).await;
    }
}
