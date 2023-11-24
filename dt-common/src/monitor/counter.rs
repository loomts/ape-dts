use std::time::Instant;

#[derive(Clone)]
pub struct Counter {
    pub timestamp: Instant,
    pub value: usize,
    pub description: String,
}

impl Counter {
    pub fn new(value: usize) -> Self {
        Self {
            timestamp: Instant::now(),
            value,
            description: String::new(),
        }
    }

    #[inline(always)]
    pub fn add(&mut self, value: usize) {
        self.value += value;
    }
}
