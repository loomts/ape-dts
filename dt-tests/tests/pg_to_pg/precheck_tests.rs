#[cfg(test)]
mod test {

    use std::collections::{HashMap, HashSet};

    use dt_precheck::meta::check_item::CheckItem;
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;
    #[tokio::test]
    #[serial]
    async fn db_not_exists_test() {
        let test_dir = "pg_to_pg/precheck/db_not_exists_test";

        let mut src_expected_results = HashMap::new();
        src_expected_results.insert(CheckItem::CheckIfStructExisted.to_string(), false);

        let mut dst_expected_results = HashMap::new();
        dst_expected_results.insert(CheckItem::CheckIfStructExisted.to_string(), true);

        run_precheck_test(test_dir, &src_expected_results, &dst_expected_results).await
    }

    #[tokio::test]
    #[serial]
    async fn db_not_exists_non_struct_test() {
        let test_dir = "pg_to_pg/precheck/db_not_exists_non_struct_test";

        let mut src_expected_results = HashMap::new();
        src_expected_results.insert(CheckItem::CheckIfStructExisted.to_string(), false);

        let mut dst_expected_results = HashMap::new();
        dst_expected_results.insert(CheckItem::CheckIfStructExisted.to_string(), false);

        run_precheck_test(test_dir, &src_expected_results, &dst_expected_results).await
    }

    #[tokio::test]
    #[serial]
    async fn struct_existed_test() {
        let test_dir = "pg_to_pg/precheck/struct_existed_test";
        run_precheck_test(test_dir, &HashMap::new(), &HashMap::new()).await
    }

    #[tokio::test]
    #[serial]
    async fn struct_supported_basic_test() {
        let test_dir = "pg_to_pg/precheck/struct_supported_basic_test";
        run_precheck_test(test_dir, &HashMap::new(), &HashMap::new()).await
    }

    #[tokio::test]
    #[serial]
    async fn struct_supported_no_pk_test() {
        let test_dir = "pg_to_pg/precheck/struct_supported_no_pk_test";

        let mut src_expected_results = HashMap::new();
        src_expected_results.insert(CheckItem::CheckIfTableStructSupported.to_string(), false);

        let mut dst_expected_results = HashMap::new();
        dst_expected_results.insert(CheckItem::CheckIfTableStructSupported.to_string(), false);

        run_precheck_test(test_dir, &src_expected_results, &dst_expected_results).await
    }

    #[tokio::test]
    #[serial]
    async fn struct_supported_have_fk_test() {
        let test_dir = "pg_to_pg/precheck/struct_supported_have_fk_test";

        let mut src_expected_results = HashMap::new();
        src_expected_results.insert(CheckItem::CheckIfTableStructSupported.to_string(), false);

        let mut dst_expected_results = HashMap::new();
        dst_expected_results.insert(CheckItem::CheckIfTableStructSupported.to_string(), false);

        run_precheck_test(test_dir, &src_expected_results, &dst_expected_results).await
    }

    async fn run_precheck_test(
        test_dir: &str,
        src_expected_results: &HashMap<String, bool>,
        dst_expected_results: &HashMap<String, bool>,
    ) {
        let mut ignore_check_items = HashSet::new();
        ignore_check_items.insert(CheckItem::CheckDatabaseVersionSupported.to_string());

        TestBase::run_precheck_test(
            test_dir,
            &ignore_check_items,
            &src_expected_results,
            &dst_expected_results,
        )
        .await
    }
}
