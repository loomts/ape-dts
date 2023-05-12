#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::tests::test_base::TestBase;

    #[test]
    #[serial]
    fn snapshot_basic_test() {
        TestBase::run_mongo_snapshot_test("mongo_to_mongo/snapshot_basic_test");
    }
}
