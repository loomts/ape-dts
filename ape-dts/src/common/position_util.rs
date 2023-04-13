use std::collections::HashMap;

use chrono::NaiveDateTime;

pub struct PositionUtil {}

impl PositionUtil {
    pub fn parse(position: &str) -> HashMap<String, String> {
        let mut result = HashMap::new();
        let tokens: Vec<&str> = position.split(",").collect();
        for token in tokens.iter() {
            let pair: Vec<&str> = token.split(":").collect();
            result.insert(pair[0].to_string(), pair[1].to_string());
        }
        result
    }

    pub fn format_timestamp_millis(millis: i64) -> String {
        let naive_datetime = NaiveDateTime::from_timestamp_millis(millis);
        naive_datetime
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S%.3f UTC-0000")
            .to_string()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_position_util_parse() {
        // mysql cdc
        let position =
            "binlog_filename:mysql-bin.000037,last_xid_position:3284004,timestamp:1679539008";
        let result = PositionUtil::parse(position);
        assert_eq!(result.get("binlog_filename").unwrap(), "mysql-bin.000037");
        assert_eq!(result.get("last_xid_position").unwrap(), "3284004");
        assert_eq!(result.get("timestamp").unwrap(), "1679539008");

        // pg cdc
        let position = "lsn:1/8FB30BB0,timestamp:1679539008";
        let result = PositionUtil::parse(position);
        assert_eq!(result.get("lsn").unwrap(), "1/8FB30BB0");
        assert_eq!(result.get("timestamp").unwrap(), "1679539008");
    }

    #[test]
    fn test_format_timestamp() {
        assert_eq!(
            "2023-03-28 07:33:48.396 UTC-0000",
            PositionUtil::format_timestamp_millis(733304028396543 / 1000 + 946_684_800 * 1000)
        );

        assert_eq!(
            "2023-03-28 05:33:47.000 UTC-0000",
            PositionUtil::format_timestamp_millis(1679981627 * 1000)
        );
    }
}
