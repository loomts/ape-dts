#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::{
        pg_to_clickhouse::table_schemas::PgFullColumnTypeTable,
        test_runner::rdb_clickhouse_test_runner::RdbClickHouseTestRunner,
    };

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        let runner = RdbClickHouseTestRunner::new("pg_to_clickhouse/snapshot/basic_test")
            .await
            .unwrap();
        runner
            .run_snapshot_test::<PgFullColumnTypeTable>()
            .await
            .unwrap();
        runner.close().await.unwrap();
    }
}
