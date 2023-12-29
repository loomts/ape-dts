use std::time::Instant;

#[derive(Clone)]
pub struct Counter {
    pub timestamp: Instant,
    pub value: usize,
    pub count: usize,
    pub description: String,
}

impl Counter {
    pub fn new(value: usize, count: usize) -> Self {
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

    #[inline(always)]
    pub fn avg_by_count(&self) -> usize {
        if self.count > 0 {
            self.value / self.count
        } else {
            0
        }
    }
}
