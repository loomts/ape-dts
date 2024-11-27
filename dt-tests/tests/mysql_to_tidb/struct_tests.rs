#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn struct_basic_test() {
        TestBase::run_mysql_struct_test("mysql_to_tidb/struct/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_charset_test() {
        TestBase::run_mysql_struct_test("mysql_to_tidb/struct/charset_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_route_test() {
        TestBase::run_mysql_struct_test("mysql_to_tidb/struct/route_test").await;
    }
}
