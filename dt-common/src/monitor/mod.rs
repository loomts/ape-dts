use async_trait::async_trait;

pub mod counter;
pub mod counter_type;
pub mod group_monitor;

#[allow(clippy::module_inception)]
pub mod monitor;
pub mod time_window_counter;

#[async_trait]
pub trait FlushableMonitor {
    async fn flush(&mut self);
}
