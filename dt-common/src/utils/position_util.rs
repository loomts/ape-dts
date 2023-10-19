use chrono::NaiveDateTime;

pub struct PositionUtil {}

impl PositionUtil {
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
