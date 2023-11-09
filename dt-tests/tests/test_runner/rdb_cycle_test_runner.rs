use dt_common::{error::Error, utils::time_util::TimeUtil};
use std::collections::HashMap;

use crate::test_config_util::TestConfigUtil;

use super::rdb_test_runner::RdbTestRunner;

pub struct RdbCycleTestRunner {
    rdb_test_runner: RdbTestRunner,
}

const DST: &str = "dst";
const TRANSACTION_TABLE_COUNT_COL: &str = "n";

impl RdbCycleTestRunner {
    pub async fn new(
        relative_test_dir: &str,
        config_override_police: &str,
        config_tmp_relative_dir: &str,
    ) -> Result<Self, Error> {
        Ok(Self {
            rdb_test_runner: RdbTestRunner::new_internal(
                relative_test_dir,
                config_override_police,
                config_tmp_relative_dir,
            )
            .await?,
        })
    }

    pub async fn run_cycle_cdc_test(
        test_dir: &str,
        start_millis: u64,
        parse_millis: u64,
        transaction_database: &str,
        expect_num_map: HashMap<String, u8>,
    ) {
        let sub_paths = TestConfigUtil::get_absolute_sub_dir(test_dir);
        let mut handlers: Vec<tokio::task::JoinHandle<()>> = vec![];
        let mut runner_map: HashMap<String, RdbCycleTestRunner> = HashMap::new();

        // init all ddls
        for sub_path in &sub_paths {
            let runner = RdbCycleTestRunner::new(
                format!("{}/{}", test_dir, sub_path.1).as_str(),
                TestConfigUtil::REPLACE_PARAM,
                sub_path.1.as_str(),
            )
            .await
            .unwrap();

            runner.initialize_ddl().await.unwrap();

            runner_map.insert(sub_path.1.to_owned(), runner);
        }

        // start task
        for sub_path in &sub_paths {
            let runner = runner_map.get(sub_path.1.as_str()).unwrap();
            handlers.push(runner.rdb_test_runner.base.spawn_task().await.unwrap());
        }
        TimeUtil::sleep_millis(start_millis).await;

        // init all datas
        for sub_path in &sub_paths {
            let runner = runner_map.get(sub_path.1.as_str()).unwrap();
            runner.initialize_data().await.unwrap();
        }
        TimeUtil::sleep_millis(parse_millis).await;

        // do check
        for sub_path in &sub_paths {
            let runner = runner_map.get(sub_path.1.as_str()).unwrap();
            let transaction_full_name = format!("{}.{}", transaction_database, sub_path.1);

            let expect_num = if expect_num_map.contains_key(sub_path.1.as_str()) {
                Some(expect_num_map.get(sub_path.1.as_str()).unwrap().clone())
            } else {
                None
            };

            runner
                .check_cycle_cdc_data(
                    String::from(transaction_database),
                    transaction_full_name,
                    expect_num,
                )
                .await
                .unwrap();
        }

        for handler in handlers {
            handler.abort();
            while !handler.is_finished() {
                TimeUtil::sleep_millis(1).await;
            }
        }
    }

    pub async fn check_cycle_cdc_data(
        &self,
        transaction_database: String,
        transaction_table_full_name: String,
        expect_num: Option<u8>,
    ) -> Result<(), Error> {
        let dml_count = match expect_num {
            Some(num) => num,
            None => self.rdb_test_runner.base.src_dml_sqls.len() as u8,
        };

        let db_tbs =
            RdbTestRunner::get_compare_db_tbs_from_sqls(&self.rdb_test_runner.base.src_ddl_sqls)?;
        let db_tbs_without_transaction: Vec<(String, String)> = db_tbs
            .iter()
            .filter(|s| !transaction_database.eq(s.0.as_str()))
            .map(|s| (s.0.clone(), s.1.clone()))
            .collect();
        assert!(
            self.rdb_test_runner
                .compare_data_for_tbs(&db_tbs_without_transaction, &db_tbs_without_transaction)
                .await?
        );

        self.check_transaction_table_data(
            DST,
            transaction_table_full_name.as_str(),
            dml_count as u8,
        )
        .await
    }

    pub async fn check_transaction_table_data(
        &self,
        from: &str,
        full_tb_name: &str,
        expect_num: u8,
    ) -> Result<(), Error> {
        let db_tb: Vec<&str> = full_tb_name.split(".").collect();
        assert_eq!(db_tb.len(), 2);

        let result = self
            .rdb_test_runner
            .fetch_data(&(db_tb[0].to_string(), db_tb[1].to_string()), from)
            .await?;

        assert!(result.len() == 1);
        let row_data = result.get(0).unwrap();
        let transaction_count = row_data
            .after
            .as_ref()
            .unwrap()
            .get(TRANSACTION_TABLE_COUNT_COL)
            .unwrap();
        assert_eq!(
            transaction_count.to_option_string(),
            Some(expect_num.to_string())
        );
        Ok(())
    }

    pub async fn initialize_ddl(&self) -> Result<(), Error> {
        // prepare src and dst tables
        self.rdb_test_runner.execute_test_ddl_sqls().await?;

        Ok(())
    }

    pub async fn initialize_data(&self) -> Result<(), Error> {
        let mut src_insert_sqls = Vec::new();
        let mut src_update_sqls = Vec::new();
        let mut src_delete_sqls = Vec::new();

        for sql in self.rdb_test_runner.base.src_dml_sqls.iter() {
            if sql.to_lowercase().starts_with("insert") {
                src_insert_sqls.push(sql.clone());
            } else if sql.to_lowercase().starts_with("update") {
                src_update_sqls.push(sql.clone());
            } else {
                src_delete_sqls.push(sql.clone());
            }
        }

        // insert src data
        self.rdb_test_runner
            .execute_src_sqls(&src_insert_sqls)
            .await?;

        // update src data
        self.rdb_test_runner
            .execute_src_sqls(&src_update_sqls)
            .await?;

        // delete src data
        self.rdb_test_runner
            .execute_src_sqls(&src_delete_sqls)
            .await?;

        Ok(())
    }
}
