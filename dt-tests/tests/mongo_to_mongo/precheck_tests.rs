#[cfg(test)]
mod test {

    use std::collections::{HashMap, HashSet};

    use dt_precheck::meta::check_item::CheckItem;
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn struct_supported_basic_test() {
        let test_dir = "mongo_to_mongo/precheck/struct_supported_basic_test";

        let mut src_expected_results = HashMap::new();
        src_expected_results.insert(CheckItem::CheckIfTableStructSupported.to_string(), false);

        let mut dst_expected_results = HashMap::new();
        dst_expected_results.insert(CheckItem::CheckIfTableStructSupported.to_string(), false);

        TestBase::run_precheck_test(
            test_dir,
            &HashSet::new(),
            &src_expected_results,
            &dst_expected_results,
        )
        .await
    }
}
