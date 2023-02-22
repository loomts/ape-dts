#[cfg(test)]
mod test {

    use futures::executor::block_on;
    use serial_test::serial;

    use crate::{
        config::rdb_to_rdb_snapshot_config::RdbToRdbSnapshotConfig, error::Error,
        task::mysql_snapshot_task::MysqlSnapshotTask, test::test_runner::TestRunner,
    };

    const TEST_DIR: &str = "src/test/mysql_to_mysql";

    #[test]
    #[serial]
    fn snapshot_basic_test() {
        let env_file = format!("{}/.env", TEST_DIR);
        let src_ddl_file = format!("{}/snapshot_basic_test/src_ddl.sql", TEST_DIR);
        let dst_ddl_file = format!("{}/snapshot_basic_test/dst_ddl.sql", TEST_DIR);
        let src_dml_file = format!("{}/snapshot_basic_test/src_dml.sql", TEST_DIR);
        let task_config_file = format!("{}/snapshot_basic_test/task_config.yaml", TEST_DIR);

        // compare src and dst data
        let cols = TestRunner::get_default_tb_cols();
        let src_tbs = TestRunner::get_default_tbs();
        let dst_tbs = TestRunner::get_default_tbs();

        let runner = block_on(TestRunner::new(&env_file)).unwrap();
        block_on(run_snapshot_test(
            &runner,
            &src_ddl_file,
            &dst_ddl_file,
            &src_dml_file,
            &task_config_file,
            &src_tbs,
            &dst_tbs,
            &cols,
        ))
        .unwrap();
    }

    async fn run_snapshot_test(
        runner: &TestRunner,
        src_ddl_file: &str,
        dst_ddl_file: &str,
        src_dml_file: &str,
        task_config_file: &str,
        src_tbs: &Vec<&str>,
        dst_tbs: &Vec<&str>,
        cols: &Vec<&str>,
    ) -> Result<(), Error> {
        // prepare src and dst tables
        runner.prepare_test_tbs(src_ddl_file, dst_ddl_file).await?;

        let src_dml_sqls = runner.load_sqls(src_dml_file).await?;

        // prepare src data
        runner
            .execute_sqls(&src_dml_sqls, &runner.src_conn_pool)
            .await?;

        // start task
        let config_str = runner.load_task_config(task_config_file).await?;
        let config = RdbToRdbSnapshotConfig::from_str(&config_str).unwrap();
        MysqlSnapshotTask { config }.start().await.unwrap();

        let res = runner
            .compare_data_for_tbs(&src_tbs, &dst_tbs, &cols)
            .await?;
        assert!(res);

        Ok(())
    }
}
