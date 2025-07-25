use async_trait::async_trait;
use dashmap::DashMap;

use super::counter::Counter;
use super::counter_type::{CounterType, WindowType};
use super::time_window_counter::TimeWindowCounter;
use super::FlushableMonitor;
use crate::log_monitor;
use crate::monitor::counter_type::AggregateType;
use crate::utils::limit_queue::LimitedQueue;

#[derive(Clone, Default)]
pub struct Monitor {
    pub name: String,
    pub description: String,
    pub no_window_counters: DashMap<CounterType, Counter>,
    pub time_window_counters: DashMap<CounterType, TimeWindowCounter>,
    pub time_window_secs: u64,
    pub max_sub_count: u64,
    pub count_window: u64,
}

#[async_trait]
impl FlushableMonitor for Monitor {
    async fn flush(&self) {
        self.flush().await;
    }
}

impl Monitor {
    pub fn new(
        name: &str,
        description: &str,
        time_window_secs: u64,
        max_sub_count: u64,
        count_window: u64,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            no_window_counters: DashMap::new(),
            time_window_counters: DashMap::new(),
            time_window_secs,
            max_sub_count,
            count_window,
        }
    }

    pub async fn flush(&self) {
        for mut entry_mut in self.time_window_counters.iter_mut() {
            let (counter_type, counter) = entry_mut.pair_mut();
            let statistics = counter.statistics();
            let mut log = format!("{} | {} | {}", self.name, self.description, counter_type);
            for aggregate_type in counter_type.get_aggregate_types() {
                let aggregate_value = match aggregate_type {
                    AggregateType::AvgByCount => statistics.avg_by_count,
                    AggregateType::AvgBySec => statistics.avg_by_sec,
                    AggregateType::Sum => statistics.sum,
                    AggregateType::MaxBySec => statistics.max_by_sec,
                    AggregateType::MaxByCount => statistics.max,
                    AggregateType::Count => statistics.count,
                    _ => continue,
                };
                log = format!("{} | {}={}", log, aggregate_type, aggregate_value);
            }
            log_monitor!("{}", log);
        }

        for entry in self.no_window_counters.iter() {
            let (counter_type, counter) = entry.pair();
            let mut log = format!("{} | {} | {}", self.name, self.description, counter_type);
            for aggregate_type in counter_type.get_aggregate_types() {
                let aggregate_value = match aggregate_type {
                    AggregateType::Latest => counter.value,
                    AggregateType::AvgByCount => counter.avg_by_count(),
                    _ => continue,
                };
                log = format!("{} | {}={}", log, aggregate_type, aggregate_value);
            }
            log_monitor!("{}", log);
        }
    }

    pub fn add_batch_counter(&self, counter_type: CounterType, value: u64, count: u64) -> &Self {
        if count == 0 {
            return self;
        }
        self.add_counter_internal(counter_type, value, count)
    }

    pub fn add_counter(&self, counter_type: CounterType, value: u64) -> &Self {
        self.add_counter_internal(counter_type, value, 1)
    }

    pub fn set_counter(&self, counter_type: CounterType, value: u64) -> &Self {
        if let WindowType::NoWindow = counter_type.get_window_type() {
            self.no_window_counters
                .entry(counter_type)
                .and_modify(|counter| counter.set(value, 1))
                .or_insert_with(|| Counter::new(value, 1));
        }
        self
    }

    pub fn add_multi_counter(
        &self,
        counter_type: CounterType,
        entry: &LimitedQueue<(u64, u64)>,
    ) -> &Self {
        self.add_muilti_counter_internal(counter_type, entry)
    }

    fn add_counter_internal(&self, counter_type: CounterType, value: u64, count: u64) -> &Self {
        match counter_type.get_window_type() {
            WindowType::NoWindow => {
                self.no_window_counters
                    .entry(counter_type)
                    .or_insert_with(|| Counter::new(0, 0))
                    .add(value, count);
            }

            WindowType::TimeWindow => {
                self.time_window_counters
                    .entry(counter_type)
                    .or_insert_with(|| {
                        TimeWindowCounter::new(self.time_window_secs, self.max_sub_count)
                    })
                    .add(value, count);
            }
        }
        self
    }

    fn add_muilti_counter_internal(
        &self,
        counter_type: CounterType,
        entry: &LimitedQueue<(u64, u64)>,
    ) -> &Self {
        match counter_type.get_window_type() {
            WindowType::NoWindow => {
                self.no_window_counters
                    .entry(counter_type)
                    .or_insert_with(|| Counter::new(0, 0))
                    .adds(entry);
            }

            WindowType::TimeWindow => {
                self.time_window_counters
                    .entry(counter_type)
                    .or_insert_with(|| {
                        TimeWindowCounter::new(self.time_window_secs, self.max_sub_count)
                    })
                    .adds(entry);
            }
        }
        self
    }
}
