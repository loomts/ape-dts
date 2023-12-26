use std::collections::HashMap;

use crate::log_monitor;
use crate::monitor::counter_type::AggreateType;

use super::counter::Counter;
use super::counter_type::{CounterType, WindowType};
use super::time_window_counter::TimeWindowCounter;

#[derive(Clone)]
pub struct Monitor {
    pub name: String,
    pub no_window_counters: HashMap<CounterType, Counter>,
    pub time_window_counters: HashMap<CounterType, TimeWindowCounter>,
    pub time_window_secs: usize,
    pub count_window: usize,
}

impl Monitor {
    pub fn new(name: &str, time_window_secs: usize, count_window: usize) -> Self {
        Self {
            name: name.into(),
            no_window_counters: HashMap::new(),
            time_window_counters: HashMap::new(),
            time_window_secs,
            count_window,
        }
    }

    pub fn flush(&mut self) {
        for (counter_type, counter) in self.time_window_counters.iter_mut() {
            counter.refresh_window();
            let aggregate = match counter_type.get_aggregate_type() {
                AggreateType::AvgByCount => counter.avg_by_count(),
                AggreateType::AvgByWindow => counter.avg_by_window(),
                AggreateType::Sum => counter.sum(),
            };
            log_monitor!(
                "{} | {} | {}",
                self.name,
                counter_type.to_string(),
                aggregate
            );
        }

        for (counter_type, counter) in self.no_window_counters.iter() {
            let aggregate = match counter_type.get_aggregate_type() {
                AggreateType::AvgByCount => counter.avg_by_count(),
                _ => counter.value,
            };
            log_monitor!(
                "{} | {} | {}",
                self.name,
                counter_type.to_string(),
                aggregate
            );
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
                    let mut counter = TimeWindowCounter::new(self.time_window_secs);
                    counter.add(value, count);
                    self.time_window_counters.insert(counter_type, counter);
                }
            }
        }
        self
    }
}
