use std::time::Instant;

use crate::utils::limit_queue::LimitedQueue;

#[derive(Clone)]
pub struct Counter {
    pub timestamp: Instant,
    pub value: u64,
    pub count: u64,
    pub description: String,
}

impl Counter {
    pub fn new(value: u64, count: u64) -> Self {
        Self {
            timestamp: Instant::now(),
            value,
            count,
            description: String::new(),
        }
    }

    #[inline(always)]
    pub fn adds(&mut self, values: &LimitedQueue<(u64, u64)>) {
        for (value, count) in values.iter() {
            if *count == 0 {
                continue;
            }
            self.add(*value, *count);
        }
    }

    #[inline(always)]
    pub fn set(&mut self, value: u64, count: u64) {
        self.value = value;
        self.count = count;
    }

    #[inline(always)]
    pub fn add(&mut self, value: u64, count: u64) {
        self.value += value;
        self.count += count;
    }

    #[inline(always)]
    pub fn avg_by_count(&self) -> u64 {
        if self.count > 0 {
            self.value / self.count
        } else {
            0
        }
    }
}
