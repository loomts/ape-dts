#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc/basic_test", 3000, 1000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_uk_changed_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc/uk_changed_test", 3000, 1000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_charset_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc/charset_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_timezone_test() {
        println!("cdc_timezone_test can be covered by test: cdc_basic_test, table: one_pk_no_uk, field: f_13 timestamp(6), the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00 ")
    }

    #[tokio::test]
    #[serial]
    async fn twoway_basic_test() {
        TestBase::run_cycle_cdc_test(
            "mysql_to_mysql/cdc/cycle_basic_test",
            3000,
            2000,
            "ape_trans_mysql",
            HashMap::new(),
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn twoway_star_test() {
        TestBase::run_cycle_cdc_test(
            "mysql_to_mysql/cdc/cycle_star_test",
            3000,
            2000,
            "ape_trans_mysql",
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
    async fn twoway_net_test() {
        TestBase::run_cycle_cdc_test(
            "mysql_to_mysql/cdc/cycle_net_test",
            3000,
            2000,
            "ape_trans_mysql",
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
