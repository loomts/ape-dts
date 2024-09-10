use chrono::Utc;

pub struct OrcSequencer {
    pub id: i64,
    pub epoch: i64,
    pub sequence: u64,
}

pub struct OrcSequenceInfo {
    pub sequencer_id: i64,
    pub push_epoch: i64,
    pub push_sequence: u64,
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

    pub fn get_sequence(&mut self) -> OrcSequenceInfo {
        if self.sequence >= 999999999 {
            // should never happen, no single table will have so big data
            self.id = Utc::now().timestamp();
            self.sequence = 0;
        }

        let sequence_info = OrcSequenceInfo {
            sequencer_id: self.id,
            push_epoch: self.epoch,
            push_sequence: self.sequence,
        };
        self.sequence += 1;
        sequence_info
    }
}

impl Default for OrcSequencer {
    fn default() -> Self {
        Self::new()
    }
}
