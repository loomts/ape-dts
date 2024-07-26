use chrono::Utc;

pub struct OrcSequencer {
    pub id: i64,
    pub epoch: i64,
    pub sequence: u64,
}

impl OrcSequencer {
    pub fn new() -> Self {
        Self {
            id: Utc::now().timestamp(),
            epoch: Utc::now().timestamp(),
            sequence: 0,
        }
    }

    pub fn update_epoch(&mut self) {
        self.epoch = Utc::now().timestamp();
    }

    pub fn get_sequence(&mut self) -> (i64, String) {
        if self.sequence >= 999999999 {
            // should never happen, no single table will have so big data
            self.id = Utc::now().timestamp();
            self.sequence = 0;
        }

        let width = 10;
        let sequence = format!("{:0>width$}_{:0>width$}", self.id, self.sequence);
        self.sequence += 1;
        (self.epoch, sequence)
    }
}

impl Default for OrcSequencer {
    fn default() -> Self {
        Self::new()
    }
}
