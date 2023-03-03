#[cfg(test)]
mod test {
    use serial_test::serial;
    use tokio::runtime::Runtime;

    use crate::test::test_runner::TestRunner;

    const TEST_DIR: &str = "src/test/pg_to_pg";

    #[test]
    #[serial]
    fn snapshot_basic_test() {
        let src_ddl_file = format!("{}/snapshot_basic_test/src_ddl.sql", TEST_DIR);
        let dst_ddl_file = format!("{}/snapshot_basic_test/dst_ddl.sql", TEST_DIR);
        let src_dml_file = format!("{}/snapshot_basic_test/src_dml.sql", TEST_DIR);
        let task_config_file = format!("{}/snapshot_basic_test/task_config.ini", TEST_DIR);

        let src_ddl_sqls = TestRunner::load_sqls(&src_ddl_file).unwrap();
        let mut src_tbs = Vec::new();
        let mut cols_list = Vec::new();
        for sql in src_ddl_sqls {
            if sql.to_lowercase().contains("create table") {
                let (tb, cols) = TestRunner::parse_create_table(&sql).unwrap();
                src_tbs.push(tb);
                cols_list.push(cols);
            }
        }
        let dst_tbs = src_tbs.clone();

        let mut k = String::new();
        for tb in &dst_tbs {
            k.push_str(format!("public.{},", tb).as_str());
        }

        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(TestRunner::new(&task_config_file)).unwrap();
        rt.block_on(runner.run_snapshot_test(
            &src_ddl_file,
            &dst_ddl_file,
            &src_dml_file,
            &task_config_file,
            &src_tbs,
            &dst_tbs,
            &cols_list,
        ))
        .unwrap();
    }
}
