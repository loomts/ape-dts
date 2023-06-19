use std::{collections::LinkedList, time::Instant};

use super::counter::Counter;

pub struct StatisticCounter {
    pub interval_secs: u64,
    pub counters: LinkedList<Counter>,
}

impl StatisticCounter {
    pub fn new(interval_secs: u64) -> Self {
        Self {
            interval_secs,
            counters: LinkedList::new(),
        }
    }

    #[inline(always)]
    pub fn add(&mut self, value: u64) {
        self.counters.push_back(Counter {
            value,
            timestamp: Instant::now(),
        });
        self.remove_outdated_counters();
    }

    #[inline(always)]
    pub fn sum(&mut self) -> u64 {
        self.remove_outdated_counters();

        let mut sum = 0;
        for counter in self.counters.iter() {
            sum += counter.value;
        }
        sum
    }

    #[inline(always)]
    pub fn avg(&mut self) -> u64 {
        self.sum() / self.interval_secs
    }

    #[inline(always)]
    fn remove_outdated_counters(&mut self) {
        let mut outdate_count = 0;
        for counter in self.counters.iter() {
            if self.check_outdate(counter) {
                outdate_count += 1;
            } else {
                break;
            }
        }

        for _ in 0..outdate_count {
            self.counters.pop_front();
        }
    }

    #[inline(always)]
    fn check_outdate(&self, counter: &Counter) -> bool {
        counter.timestamp.elapsed().as_secs() > self.interval_secs
    }
}
