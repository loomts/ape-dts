#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn struct_basic_test() {
        TestBase::run_mysql_struct_test("pg_to_starrocks/struct/3_2_11/basic_test").await;
    }
}
