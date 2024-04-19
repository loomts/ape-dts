use crate::utils::time_util::TimeUtil;

#[derive(Default)]
pub struct TimeFilter {
    // timestamp in UTC
    pub start_timestamp: u32,
    pub end_timestamp: u32,
    pub started: bool,
    pub ended: bool,
}

impl TimeFilter {
    pub fn new(start_time_utc: &str, end_time_utc: &str) -> Self {
        let start_timestamp = if start_time_utc.is_empty() {
            0
        } else {
            TimeUtil::datetime_from_utc_str(start_time_utc)
                .unwrap()
                .timestamp() as u32
        };

        let end_timestamp = if end_time_utc.is_empty() {
            u32::MAX
        } else {
            TimeUtil::datetime_from_utc_str(end_time_utc)
                .unwrap()
                .timestamp() as u32
        };

        Self {
            start_timestamp,
            end_timestamp,
            started: start_time_utc.is_empty(),
            ended: false,
        }
    }
}
