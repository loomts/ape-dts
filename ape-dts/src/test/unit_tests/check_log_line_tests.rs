#[cfg(test)]
mod test {
    use crate::common::check_log_line::CheckLogLine;

    #[test]
    fn check_log_line_test() {
        let str = "db1,tb1,col1,4,1234,col2,5,abcde";
        let line = CheckLogLine::from_string(str.to_string());
        assert_eq!(line.schema, "db1");
        assert_eq!(line.tb, "tb1");
        assert_eq!(line.cols, vec!["col1", "col2"]);
        assert_eq!(line.col_values, vec!["1234", "abcde"]);
        assert_eq!(str, line.to_string());

        let str = "db1,tb1";
        let line = CheckLogLine::from_string(str.to_string());
        assert_eq!(line.schema, "db1");
        assert_eq!(line.tb, "tb1");
        assert_eq!(line.cols.len(), 0);
        assert_eq!(line.col_values.len(), 0);
        assert_eq!(str, line.to_string());

        let str = "db1";
        let line = CheckLogLine::from_string(str.to_string());
        assert_eq!(line.schema, "db1");
        assert_eq!(line.tb, "");
        assert_eq!(line.cols.len(), 0);
        assert_eq!(line.col_values.len(), 0);

        let str = "";
        let line = CheckLogLine::from_string(str.to_string());
        assert_eq!(line.schema, "");
        assert_eq!(line.tb, "");
        assert_eq!(line.cols.len(), 0);
        assert_eq!(line.col_values.len(), 0);
    }
}
