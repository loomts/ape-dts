use std::{cmp, time::Instant};

#[derive(Clone)]
pub struct Counter {
    pub timestamp: Instant,
    pub value: usize,
    pub count: usize,
    pub description: String,
}

impl Counter {
    pub fn new(value: usize, count: usize) -> Self {
        let count = cmp::max(1, count);
        Self {
            timestamp: Instant::now(),
            value,
            count,
            description: String::new(),
        }
    }

    #[inline(always)]
    pub fn add(&mut self, value: usize, count: usize) {
        self.value += value;
        self.count += count;
    }
}
