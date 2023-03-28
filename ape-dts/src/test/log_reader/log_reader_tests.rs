#[cfg(test)]
mod test {
    use crate::{common::log_reader::LogReader, test::test_config_util::TestConfigUtil};

    #[test]
    fn log_reader_empty_test() {
        let dir = TestConfigUtil::get_absolute_dir("log_reader/log_reader_empty_test");
        let mut reader = LogReader::new(&dir);
        assert_eq!(reader.next(), None);
    }

    #[test]
    fn log_reader_one_log_test() {
        let dir = TestConfigUtil::get_absolute_dir("log_reader/log_reader_one_log_test");
        let mut reader = LogReader::new(&dir);
        assert_eq!(reader.next().unwrap(), "log1.line1");
        assert_eq!(reader.next().unwrap(), "");
        assert_eq!(reader.next().unwrap(), "log1.line3");
        assert_eq!(reader.next(), None);
    }

    #[test]
    fn log_reader_multi_log_test() {
        let dir = TestConfigUtil::get_absolute_dir("log_reader/log_reader_multi_log_test");
        let mut reader = LogReader::new(&dir);
        assert_eq!(reader.next().unwrap(), "log1.line1");
        assert_eq!(reader.next().unwrap(), "");
        assert_eq!(reader.next().unwrap(), "log1.line3");
        assert_eq!(reader.next().unwrap(), "");

        assert_eq!(reader.next().unwrap(), "log2.line2");
        assert_eq!(reader.next().unwrap(), "");
        assert_eq!(reader.next().unwrap(), "log2.line4");
        assert_eq!(reader.next().unwrap(), "");

        assert_eq!(reader.next().unwrap(), "");
        assert_eq!(reader.next(), None);
    }
}
