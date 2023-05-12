use std::time::Instant;

pub struct Counter {
    pub timestamp: Instant,
    pub value: u64,
}

impl Counter {
    pub fn new() -> Self {
        Self {
            timestamp: Instant::now(),
            value: 0,
        }
    }

    #[inline(always)]
    pub fn add(&mut self, value: u64) {
        self.value += value;
    }
}
