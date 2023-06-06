use std::time::Duration;

pub struct TimeUtil {}

impl TimeUtil {
    #[inline(always)]
    pub async fn sleep_millis(millis: u64) {
        tokio::time::sleep(Duration::from_millis(millis)).await;
    }
}
