use std::collections::LinkedList;

use super::counter::Counter;

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
    pub fn sum(&mut self) -> usize {
        let mut sum = 0;
        for counter in self.counters.iter() {
            sum += counter.value;
        }
        sum
    }

    #[inline(always)]
    pub fn count(&mut self) -> usize {
        let mut count = 0;
        for counter in self.counters.iter() {
            count += counter.count;
        }
        count
    }

    #[inline(always)]
    pub fn avg_by_interval(&mut self) -> usize {
        self.sum() / self.time_window_secs
    }

    #[inline(always)]
    pub fn avg_by_count(&mut self) -> usize {
        if self.counters.len() > 0 {
            self.sum() / self.count()
        } else {
            0
        }
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
