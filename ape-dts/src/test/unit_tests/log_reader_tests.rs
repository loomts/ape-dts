#[cfg(test)]
mod test {
    use crate::{common::log_reader::LogReader, test::test_config_util::TestConfigUtil};

    #[test]
    fn log_reader_test_0() {
        let dir = TestConfigUtil::get_absolute_dir("unit_tests/log_reader_test_0");
        let mut reader = LogReader::new(&dir);
        assert_eq!(reader.next(), None);
    }

    #[test]
    fn log_reader_test_1() {
        let dir = TestConfigUtil::get_absolute_dir("unit_tests/log_reader_test_1");
        let mut reader = LogReader::new(&dir);
        assert_eq!(reader.next().unwrap(), "log1.line1");
        assert_eq!(reader.next().unwrap(), "");
        assert_eq!(reader.next().unwrap(), "log1.line3");
        assert_eq!(reader.next(), None);
    }

    #[test]
    fn log_reader_test_2() {
        let dir = TestConfigUtil::get_absolute_dir("unit_tests/log_reader_test_2");
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
