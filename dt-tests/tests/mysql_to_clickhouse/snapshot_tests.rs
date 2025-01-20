#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::{
        mysql_to_clickhouse::table_schemas::{MysqlBasicTable, MysqlRouteTable},
        test_runner::rdb_clickhouse_test_runner::RdbClickHouseTestRunner,
    };

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        let runner = RdbClickHouseTestRunner::new("mysql_to_clickhouse/snapshot/basic_test")
            .await
            .unwrap();
        runner.run_snapshot_test::<MysqlBasicTable>().await.unwrap();
        runner.close().await.unwrap();
    }

    // TODO, This test will fail, caused by ClickHouse client (fetch_all function)
    // #[tokio::test]
    #[serial]
    async fn snapshot_route_test() {
        let runner = RdbClickHouseTestRunner::new("mysql_to_clickhouse/snapshot/route_test")
            .await
            .unwrap();
        runner.run_snapshot_test::<MysqlRouteTable>().await.unwrap();
        runner.close().await.unwrap();
    }
}
