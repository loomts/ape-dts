#[cfg(test)]
mod test {

    use dt_connector::check_log::log_reader::LogReader;
    use serial_test::serial;

    use crate::test_config_util::TestConfigUtil;

    #[test]
    #[serial]
    fn log_reader_empty_test() {
        let dir = TestConfigUtil::get_absolute_dir("log_reader/log_reader_empty_test");
        let mut reader = LogReader::new(&dir);
        assert_eq!(reader.nextval(), None);
    }

    #[test]
    #[serial]
    fn log_reader_one_log_test() {
        let dir = TestConfigUtil::get_absolute_dir("log_reader/log_reader_one_log_test");
        let mut reader = LogReader::new(&dir);
        assert_eq!(reader.nextval().unwrap(), "log1.line1");
        assert_eq!(reader.nextval().unwrap(), "");
        assert_eq!(reader.nextval().unwrap(), "log1.line3");
        assert_eq!(reader.nextval(), None);
    }

    #[test]
    #[serial]
    fn log_reader_multi_log_test() {
        let dir = TestConfigUtil::get_absolute_dir("log_reader/log_reader_multi_log_test");
        let mut reader = LogReader::new(&dir);
        assert_eq!(reader.nextval().unwrap(), "log1.line1");
        assert_eq!(reader.nextval().unwrap(), "");
        assert_eq!(reader.nextval().unwrap(), "log1.line3");
        assert_eq!(reader.nextval().unwrap(), "");

        assert_eq!(reader.nextval().unwrap(), "log2.line2");
        assert_eq!(reader.nextval().unwrap(), "");
        assert_eq!(reader.nextval().unwrap(), "log2.line4");
        assert_eq!(reader.nextval().unwrap(), "");

        assert_eq!(reader.nextval().unwrap(), "");
        assert_eq!(reader.nextval(), None);
    }
}
