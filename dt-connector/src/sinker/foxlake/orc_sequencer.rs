use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;

pub struct OrcSequencer {
    pub id: i64,
    pub sequence: AtomicU64,
}

impl OrcSequencer {
    pub fn new() -> Self {
        Self {
            id: Utc::now().timestamp(),
            sequence: AtomicU64::new(0),
        }
    }

    pub fn get_sequence(&mut self) -> String {
        if self.sequence.load(Ordering::Acquire) >= 999999999 {
            // should never happen, no single table will have so big data
            self.id = Utc::now().timestamp();
            self.sequence.store(0, Ordering::Release);
        }

        let width = 10;
        format!(
            "{:0>width$}_{:0>width$}",
            self.id,
            self.sequence.fetch_add(1, Ordering::Release)
        )
    }
}
