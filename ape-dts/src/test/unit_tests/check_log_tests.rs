#[cfg(test)]
mod test {
    use crate::common::check_log::CheckLog;

    #[test]
    fn check_log_test() {
        let str = "db1,tb1,col1,4,1234,col2,5,abcde";
        let log = CheckLog::from_string(str.to_string());
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "tb1");
        assert_eq!(log.cols, vec!["col1", "col2"]);
        assert_eq!(log.col_values, vec!["1234", "abcde"]);
        assert_eq!(str, log.to_string());

        let str = "db1,tb1";
        let log = CheckLog::from_string(str.to_string());
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "tb1");
        assert_eq!(log.cols.len(), 0);
        assert_eq!(log.col_values.len(), 0);
        assert_eq!(str, log.to_string());

        let str = "db1";
        let log = CheckLog::from_string(str.to_string());
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "");
        assert_eq!(log.cols.len(), 0);
        assert_eq!(log.col_values.len(), 0);

        let str = "";
        let log = CheckLog::from_string(str.to_string());
        assert_eq!(log.schema, "");
        assert_eq!(log.tb, "");
        assert_eq!(log.cols.len(), 0);
        assert_eq!(log.col_values.len(), 0);
    }
}
