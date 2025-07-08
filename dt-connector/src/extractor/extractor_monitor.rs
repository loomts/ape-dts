use std::sync::Arc;

use tokio::{sync::Mutex, time::Instant};

use dt_common::monitor::{counter_type::CounterType, monitor::Monitor};

#[derive(Clone, Default)]
pub struct ExtractorCounters {
    pub record_count: usize,
    pub data_size: usize,
}

impl ExtractorCounters {
    pub fn new() -> Self {
        Self {
            record_count: 0,
            data_size: 0,
        }
    }
}

pub struct ExtractorMonitor {
    pub monitor: Arc<Mutex<Monitor>>,
    pub count_window: usize,
    pub time_window_secs: usize,
    pub last_flush_time: Instant,
    pub flushed_counters: ExtractorCounters,
    pub counters: ExtractorCounters,
}

impl ExtractorMonitor {
    pub async fn new(monitor: Arc<Mutex<Monitor>>) -> Self {
        let count_window = monitor.lock().await.count_window;
        let time_window_secs = monitor.lock().await.time_window_secs;
        Self {
            monitor,
            last_flush_time: Instant::now(),
            count_window,
            time_window_secs,
            flushed_counters: ExtractorCounters::new(),
            counters: ExtractorCounters::new(),
        }
    }

    pub async fn try_flush(&mut self, force: bool) {
        let record_count = self.counters.record_count - self.flushed_counters.record_count;
        let record_size = self.counters.data_size - self.flushed_counters.data_size;
        // to avoid too many sub counters, add counter by batch
        if force
            || record_count >= self.count_window
            || self.last_flush_time.elapsed().as_secs() >= self.time_window_secs as u64
        {
            self.monitor
                .lock()
                .await
                .add_counter(CounterType::RecordCount, record_count)
                .add_counter(CounterType::DataBytes, record_size);
            self.last_flush_time = Instant::now();
            self.flushed_counters = self.counters.clone();
        }
    }
}
