use std::{cmp, collections::LinkedList};

use super::counter::Counter;

#[derive(Default)]
pub struct WindowCounterStatistic {
    pub sum: usize,
    pub max: usize,
    pub max_by_sec: usize,
    pub count: usize,
    pub avg_by_count: usize,
    pub avg_by_sec: usize,
}

#[derive(Clone)]
pub struct TimeWindowCounter {
    pub time_window_secs: usize,
    pub counters: LinkedList<Counter>,
    pub description: String,
}

impl TimeWindowCounter {
    pub fn new(time_window_secs: usize) -> Self {
        Self {
            time_window_secs,
            counters: LinkedList::new(),
            description: String::new(),
        }
    }

    #[inline(always)]
    pub fn add(&mut self, value: usize, count: usize) {
        self.counters.push_back(Counter::new(value, count));
    }

    #[inline(always)]
    pub fn statistics(&mut self) -> WindowCounterStatistic {
        self.refresh_window();

        let mut statistics = WindowCounterStatistic {
            ..Default::default()
        };

        let mut sum_in_current_sec = 0;
        let mut current_elapsed_secs = 0;

        for counter in self.counters.iter() {
            statistics.sum += counter.value;
            statistics.count += counter.count;
            statistics.max = cmp::max(statistics.max, counter.value);

            if current_elapsed_secs == counter.timestamp.elapsed().as_secs() {
                sum_in_current_sec += counter.value;
            } else {
                current_elapsed_secs = counter.timestamp.elapsed().as_secs();
                statistics.max_by_sec = cmp::max(statistics.max_by_sec, sum_in_current_sec);
                sum_in_current_sec = counter.value;
            }
        }
        statistics.max_by_sec = cmp::max(statistics.max_by_sec, sum_in_current_sec);

        if statistics.count > 0 {
            statistics.avg_by_count = statistics.sum / statistics.count;
            statistics.avg_by_sec = statistics.sum / self.time_window_secs;
        }

        statistics
    }

    #[inline(always)]
    pub fn refresh_window(&mut self) {
        let mut outdate_count = 0;
        for counter in self.counters.iter() {
            if counter.timestamp.elapsed().as_secs() >= self.time_window_secs as u64 {
                outdate_count += 1;
            } else {
                break;
            }
        }

        for _ in 0..outdate_count {
            self.counters.pop_front();
        }
    }
}
