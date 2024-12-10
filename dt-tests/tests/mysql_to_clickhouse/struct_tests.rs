#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::rdb_clickhouse_test_runner::RdbClickHouseTestRunner;

    #[tokio::test]
    #[serial]
    async fn struct_basic_test() {
        let runner = RdbClickHouseTestRunner::new("mysql_to_clickhouse/struct/basic_test")
            .await
            .unwrap();
        runner.run_struct_test().await.unwrap();
        runner.close().await.unwrap();
    }
}
