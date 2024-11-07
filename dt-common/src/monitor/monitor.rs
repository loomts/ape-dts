use std::collections::HashMap;

use crate::log_monitor;
use crate::monitor::counter_type::AggregateType;

use super::counter::Counter;
use super::counter_type::{CounterType, WindowType};
use super::time_window_counter::TimeWindowCounter;

#[derive(Clone, Default)]
pub struct Monitor {
    pub name: String,
    pub no_window_counters: HashMap<CounterType, Counter>,
    pub time_window_counters: HashMap<CounterType, TimeWindowCounter>,
    pub time_window_secs: usize,
    pub max_sub_count: usize,
    pub count_window: usize,
}

impl Monitor {
    pub fn new(
        name: &str,
        time_window_secs: usize,
        max_sub_count: usize,
        count_window: usize,
    ) -> Self {
        Self {
            name: name.into(),
            no_window_counters: HashMap::new(),
            time_window_counters: HashMap::new(),
            time_window_secs,
            max_sub_count,
            count_window,
        }
    }

    pub fn flush(&mut self) {
        for (counter_type, counter) in self.time_window_counters.iter_mut() {
            let statistics = counter.statistics();
            let mut log = format!("{} | {}", self.name, counter_type);
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

        for (counter_type, counter) in self.no_window_counters.iter() {
            let mut log = format!("{} | {}", self.name, counter_type);
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

    pub fn add_batch_counter(
        &mut self,
        counter_type: CounterType,
        value: usize,
        count: usize,
    ) -> &mut Self {
        if count == 0 {
            return self;
        }
        self.add_counter_internal(counter_type, value, count)
    }

    pub fn add_counter(&mut self, counter_type: CounterType, value: usize) -> &mut Self {
        self.add_counter_internal(counter_type, value, 1)
    }

    fn add_counter_internal(
        &mut self,
        counter_type: CounterType,
        value: usize,
        count: usize,
    ) -> &mut Self {
        match counter_type.get_window_type() {
            WindowType::NoWindow => {
                if let Some(counter) = self.no_window_counters.get_mut(&counter_type) {
                    counter.add(value, count);
                } else {
                    self.no_window_counters
                        .insert(counter_type, Counter::new(value, count));
                }
            }

            WindowType::TimeWindow => {
                if let Some(counter) = self.time_window_counters.get_mut(&counter_type) {
                    counter.add(value, count)
                } else {
                    let mut counter =
                        TimeWindowCounter::new(self.time_window_secs, self.max_sub_count);
                    counter.add(value, count);
                    self.time_window_counters.insert(counter_type, counter);
                }
            }
        }
        self
    }
}
