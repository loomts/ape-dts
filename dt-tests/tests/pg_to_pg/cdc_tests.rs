#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/basic_test", 7000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_postgis_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/postgis_test", 7000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_postgis_array_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/postgis_array_test", 7000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_charset_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/charset_test", 7000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_charset_euc_cn_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/charset_euc_cn_test", 7000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_timezone_test() {
        println!("cdc_timezone_test can be covered by test: cdc_basic_test, table: timezone_table, the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00")
    }
}
