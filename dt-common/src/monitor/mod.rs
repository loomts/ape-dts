pub mod counter;
pub mod counter_type;
pub mod group_monitor;
#[allow(clippy::module_inception)]
pub mod monitor;
pub mod time_window_counter;

pub trait FlushableMonitor {
    fn flush(&mut self);
}
