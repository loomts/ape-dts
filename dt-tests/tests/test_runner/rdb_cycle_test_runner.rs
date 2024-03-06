use dt_common::{error::Error, utils::time_util::TimeUtil};
use dt_connector::data_marker::DataMarker;
use std::collections::HashMap;
use tokio::task::JoinHandle;

use crate::test_config_util::TestConfigUtil;

use super::rdb_test_runner::RdbTestRunner;

pub struct RdbCycleTestRunner {
    base: RdbTestRunner,
}

const DST: &str = "dst";

impl RdbCycleTestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        Ok(Self {
            base: RdbTestRunner::new(relative_test_dir, false).await?,
        })
    }

    pub async fn close(&self) -> Result<(), Error> {
        self.base.close().await
    }

    pub fn build_expect_tx_count_map(
        tx_check_data: &Vec<(&str, &str, &str, &str)>,
    ) -> HashMap<(String, String, String), u8> {
        let mut expect_tx_count_map = HashMap::new();
        for (src_node, dst_node, data_origin_node, expect_tx_count) in tx_check_data {
            let expect_tx_count: u8 = expect_tx_count.parse().unwrap();
            expect_tx_count_map.insert(
                (
                    src_node.to_string(),
                    dst_node.to_string(),
                    data_origin_node.to_string(),
                ),
                expect_tx_count,
            );
        }
        expect_tx_count_map
    }

    pub async fn run_cycle_cdc_test(
        test_dir: &str,
        start_millis: u64,
        parse_millis: u64,
        tx_check_data: &Vec<(&str, &str, &str, &str)>,
    ) {
        // HashMap<(src_node, dst_node, data_origin_node), expect_tx_count>
        let expect_tx_count_map = Self::build_expect_tx_count_map(tx_check_data);

        let sub_paths = TestConfigUtil::get_absolute_sub_dir(test_dir);
        let mut handlers: Vec<JoinHandle<()>> = vec![];
        let mut runner_map: HashMap<String, RdbCycleTestRunner> = HashMap::new();

        // execute all prepare sqls
        for sub_path in &sub_paths {
            let runner = Self::new(format!("{}/{}", test_dir, sub_path.1).as_str())
                .await
                .unwrap();
            runner.base.execute_prepare_sqls().await.unwrap();
            runner_map.insert(sub_path.1.to_owned(), runner);
        }

        // start all sub tasks
        for sub_path in &sub_paths {
            let runner = runner_map.get(sub_path.1.as_str()).unwrap();
            handlers.push(runner.base.base.spawn_task().await.unwrap());
            TimeUtil::sleep_millis(start_millis).await;
        }

        // execute all test sqls
        for sub_path in &sub_paths {
            let runner = runner_map.get(sub_path.1.as_str()).unwrap();
            runner.init_data().await.unwrap();
        }
        TimeUtil::sleep_millis(parse_millis).await;

        // do check
        for sub_path in &sub_paths {
            let runner = runner_map.get(sub_path.1.as_str()).unwrap();
            let data_marker = runner.get_data_marker();

            let tmp_expect_tx_count_map: HashMap<(String, String, String), u8> =
                expect_tx_count_map
                    .clone()
                    .into_iter()
                    .filter(|i| i.0 .0 == data_marker.src_node && i.0 .1 == data_marker.dst_node)
                    .collect();

            runner
                .check_cycle_cdc_data(&tmp_expect_tx_count_map)
                .await
                .unwrap();
        }

        for handler in handlers {
            handler.abort();
            while !handler.is_finished() {
                TimeUtil::sleep_millis(1).await;
            }
        }

        for (_, runner) in runner_map {
            runner.close().await.unwrap();
        }
    }

    async fn check_cycle_cdc_data(
        &self,
        // HashMap<(src_node, dst_node, data_origin_node), expect_tx_count>
        expect_tx_count_map: &HashMap<(String, String, String), u8>,
    ) -> Result<(), Error> {
        let data_marker = self.get_data_marker();
        let mut db_tbs =
            RdbTestRunner::get_compare_db_tbs_from_sqls(&self.base.base.src_prepare_sqls)?;
        for i in 0..db_tbs.len() {
            if db_tbs[i].0 == data_marker.marker_db && db_tbs[i].1 == data_marker.marker_tb {
                db_tbs.remove(i);
                break;
            }
        }

        assert!(self.base.compare_data_for_tbs(&db_tbs, &db_tbs).await?);

        self.check_data_marker_data(expect_tx_count_map).await
    }

    async fn check_data_marker_data(
        &self,
        // HashMap<(src_node, dst_node, data_origin_node), expect_tx_count>
        expect_tx_count_map: &HashMap<(String, String, String), u8>,
    ) -> Result<(), Error> {
        let data_marker = self.get_data_marker();
        let result = self
            .base
            .fetch_data(
                &(
                    data_marker.marker_db.to_string(),
                    data_marker.marker_tb.to_string(),
                ),
                DST,
            )
            .await?;

        for row_data in result {
            let after = row_data.after.as_ref().unwrap();

            let src_node = after.get("src_node").unwrap().to_string();
            let dst_node = after.get("dst_node").unwrap().to_string();
            let data_origin_node = after.get("data_origin_node").unwrap().to_string();
            let tx_count: u8 = after.get("n").unwrap().to_string().parse().unwrap();

            if src_node != data_marker.src_node || dst_node != data_marker.dst_node {
                continue;
            }

            println!(
                "src_node: {}, dst_node: {}, data_origin_node: {}, tx_count: {}",
                src_node, dst_node, data_origin_node, tx_count
            );

            assert_eq!(
                tx_count,
                *expect_tx_count_map
                    .get(&(src_node, dst_node, data_origin_node))
                    .unwrap()
            );
        }
        Ok(())
    }

    async fn init_data(&self) -> Result<(), Error> {
        let mut src_insert_sqls = Vec::new();
        let mut src_update_sqls = Vec::new();
        let mut src_delete_sqls = Vec::new();

        for sql in self.base.base.src_test_sqls.iter() {
            if sql.to_lowercase().starts_with("insert") {
                src_insert_sqls.push(sql.clone());
            } else if sql.to_lowercase().starts_with("update") {
                src_update_sqls.push(sql.clone());
            } else {
                src_delete_sqls.push(sql.clone());
            }
        }

        // insert src data
        self.base.execute_src_sqls(&src_insert_sqls).await?;

        // update src data
        self.base.execute_src_sqls(&src_update_sqls).await?;

        // delete src data
        self.base.execute_src_sqls(&src_delete_sqls).await?;

        Ok(())
    }

    fn get_data_marker(&self) -> DataMarker {
        self.base.base.get_data_marker().unwrap()
    }
}
