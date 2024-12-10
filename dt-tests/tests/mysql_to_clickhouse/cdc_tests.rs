#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::{
        mysql_to_clickhouse::table_schemas::MysqlBasicTable,
        test_runner::rdb_clickhouse_test_runner::RdbClickHouseTestRunner,
    };

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        let runner = RdbClickHouseTestRunner::new("mysql_to_clickhouse/cdc/basic_test")
            .await
            .unwrap();
        runner
            .run_cdc_test::<MysqlBasicTable>(3000, 5000)
            .await
            .unwrap();
        runner.close().await.unwrap();
    }
}
