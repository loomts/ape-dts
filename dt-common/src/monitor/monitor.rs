use std::collections::HashMap;

use strum::{Display, EnumString, IntoStaticStr};

use super::counter::Counter;
use super::time_window_counter::TimeWindowCounter;

#[derive(Clone)]
pub struct Monitor {
    pub accumulate_counters: HashMap<CounterType, Counter>,
    pub time_window_counters: HashMap<CounterType, TimeWindowCounter>,
    pub time_window_secs: usize,
}

#[derive(EnumString, IntoStaticStr, Display, PartialEq, Eq, Hash, Clone)]
pub enum CounterType {
    // time window counter, aggregate by: sum by interval
    #[strum(serialize = "batch_write_failures")]
    BatchWriteFailures,
    #[strum(serialize = "serial_writes")]
    SerialWrites,

    // time window counter, aggregate by: avg by interval
    #[strum(serialize = "rps")]
    Records,

    // time window counter, aggregate by: avg by count
    #[strum(serialize = "bytes_per_query")]
    BytesPerQuery,
    #[strum(serialize = "records_per_query")]
    RecordsPerQuery,
    #[strum(serialize = "rt_per_query")]
    RtPerQuery,
    #[strum(serialize = "buffer_size")]
    BufferSize,

    // accumulate counter
    #[strum(serialize = "sinked_count")]
    SinkedCount,
}

const DEFAULT_INTERVAL_SECS: usize = 5;

impl Monitor {
    pub fn new_default() -> Self {
        Self::new(DEFAULT_INTERVAL_SECS)
    }

    pub fn new(interval_secs: usize) -> Self {
        Self {
            accumulate_counters: HashMap::new(),
            time_window_counters: HashMap::new(),
            time_window_secs: interval_secs,
        }
    }

    pub fn add_counter(&mut self, counter_type: CounterType, value: usize) -> &mut Self {
        match counter_type {
            CounterType::SinkedCount => {
                if let Some(counter) = self.accumulate_counters.get_mut(&counter_type) {
                    counter.add(value)
                } else {
                    self.accumulate_counters
                        .insert(counter_type, Counter::new(value));
                }
            }

            _ => {
                if let Some(counter) = self.time_window_counters.get_mut(&counter_type) {
                    counter.add(value)
                } else {
                    let mut counter = TimeWindowCounter::new(self.time_window_secs);
                    counter.add(value);
                    self.time_window_counters.insert(counter_type, counter);
                }
            }
        }
        self
    }
}
