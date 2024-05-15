use std::time::Duration;

use anyhow::{bail, Context};
use chrono::{DateTime, NaiveDateTime, Utc};

pub struct TimeUtil {}

const UTC_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%z";

impl TimeUtil {
    #[inline(always)]
    pub async fn sleep_millis(millis: u64) {
        tokio::time::sleep(Duration::from_millis(millis)).await;
    }

    #[inline(always)]
    pub fn datetime_from_utc_str(str: &str) -> anyhow::Result<DateTime<Utc>> {
        let datetime = NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S")
            .with_context(|| format!("datetime_from_utc_str failed, input: [{}]", str))?
            .and_utc();
        Ok(datetime)
    }

    #[inline(always)]
    pub fn timestamp_to_str(timestamp: u32) -> anyhow::Result<String> {
        if let Some(datetime) = DateTime::from_timestamp(timestamp as i64, 0) {
            Ok(datetime.format(UTC_FORMAT).to_string())
        } else {
            bail!(format!("timestamp_to_str failed, input: [{}]", timestamp))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_datetime_from_utc_str() {
        let input = "2024-04-17 12:34:56";
        let datetime = TimeUtil::datetime_from_utc_str(&input).unwrap();
        assert_eq!(
            datetime.format(UTC_FORMAT).to_string(),
            "2024-04-17T12:34:56+0000"
        )
    }
}
