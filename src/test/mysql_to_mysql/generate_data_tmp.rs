#[cfg(test)]
mod test {
    use serial_test::serial;
    use tokio::runtime::Runtime;

    use crate::{error::Error, test::test_runner::TestRunner};

    const TEST_DIR: &str = "src/test/mysql_to_mysql";

    #[test]
    #[serial]
    fn test() {
        let env_file = format!("{}/.env", TEST_DIR);
        let src_dml_file = format!("{}/generate_data_tmp/src_dml.sql", TEST_DIR);
        let rt = Runtime::new().unwrap();
        rt.block_on(run_test(&env_file, &src_dml_file)).unwrap();
    }

    async fn run_test(env_file: &str, src_dml_file: &str) -> Result<(), Error> {
        let runner = TestRunner::new(&env_file).await?;

        // load dml sqls
        let src_dml_sqls_tmp = runner.load_sqls(src_dml_file).await?;
        let mut src_dml_sqls: Vec<String> = Vec::new();
        for _ in 0..10000 {
            src_dml_sqls.append(&mut src_dml_sqls_tmp.clone());
        }

        let mut src_insert_sqls = Vec::new();
        let mut src_update_sqls = Vec::new();
        let mut src_delete_sqls = Vec::new();

        for mut sql in src_dml_sqls {
            sql = sql.to_lowercase();
            if sql.starts_with("insert") {
                src_insert_sqls.push(sql);
            } else if sql.starts_with("update") {
                src_update_sqls.push(sql);
            } else {
                src_delete_sqls.push(sql);
            }
        }

        // insert src data
        runner
            .execute_sqls(&src_insert_sqls, &runner.src_conn_pool)
            .await
    }
}
