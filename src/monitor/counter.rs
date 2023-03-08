use std::time::Instant;

pub struct Counter {
    pub timestamp: Instant,
    pub value: u64,
}
