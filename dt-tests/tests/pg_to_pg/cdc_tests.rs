#[cfg(test)]
mod test {

    use std::collections::HashMap;

    use serial_test::serial;

    use crate::test_runner::{rdb_cycle_test_runner::RdbCycleTestRunner, test_base::TestBase};

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

    #[tokio::test]
    #[serial]
    async fn cdc_special_character_in_name_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/special_character_in_name_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_wildchar_filter_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/wildchar_filter_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_route_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/route_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_foreign_key_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/foreign_key_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn circle_basic_test() {
        RdbCycleTestRunner::run_cycle_cdc_test(
            "pg_to_pg/cdc/cycle_basic_test",
            3000,
            2000,
            "ape_trans_pg",
            HashMap::new(),
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn circle_star_test() {
        RdbCycleTestRunner::run_cycle_cdc_test(
            "pg_to_pg/cdc/cycle_star_test",
            3000,
            2000,
            "ape_trans_pg",
            vec![
                ("topo1_node1_to_node2".to_string(), 20 as u8),
                ("topo1_node1_to_node3".to_string(), 20 as u8),
                ("topo1_node2_to_node1".to_string(), 10 as u8),
                ("topo1_node3_to_node1".to_string(), 10 as u8),
            ]
            .into_iter()
            .collect::<HashMap<String, u8>>(),
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn circle_net_test() {
        RdbCycleTestRunner::run_cycle_cdc_test(
            "pg_to_pg/cdc/cycle_net_test",
            3000,
            2000,
            "ape_trans_pg",
            vec![
                ("topo1_node1_to_node2".to_string(), 10 as u8),
                ("topo1_node1_to_node3".to_string(), 10 as u8),
                ("topo1_node2_to_node1".to_string(), 10 as u8),
                ("topo1_node2_to_node3".to_string(), 10 as u8),
                ("topo1_node3_to_node1".to_string(), 10 as u8),
                ("topo1_node3_to_node2".to_string(), 10 as u8),
            ]
            .into_iter()
            .collect::<HashMap<String, u8>>(),
        )
        .await;
    }
}
