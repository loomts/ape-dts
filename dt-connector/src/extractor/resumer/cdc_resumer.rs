use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use dt_common::{
    config::resumer_config::ResumerConfig, meta::position::Position, utils::file_util::FileUtil,
};
use serde_json::json;

use super::{CURRENT_POSITION_LOG_FLAG, TAIL_POSITION_COUNT};

#[derive(Clone, Default)]
pub struct CdcResumer {
    pub current_position: Position,
    pub checkpoint_position: Position,
}

impl CdcResumer {
    pub fn from_config(config: &ResumerConfig) -> anyhow::Result<Self> {
        let mut me = Self::default();

        if let Ok(file) = File::open(&config.resume_config_file) {
            for line in BufReader::new(file).lines().map_while(Result::ok) {
                me.load_resume_line(&line);
            }
            me.current_position = me.checkpoint_position.clone();
        }

        if config.resume_from_log {
            let position_log = format!("{}/position.log", config.resume_log_dir);
            if let Ok(lines) = FileUtil::tail(&position_log, TAIL_POSITION_COUNT) {
                for line in lines.iter() {
                    me.load_resume_line(line)
                }
            }
        }
        Ok(me)
    }

    fn load_resume_line(&mut self, line: &str) {
        let position = Position::from_log(line);
        if position == Position::None {
            return;
        }

        if line.contains(CURRENT_POSITION_LOG_FLAG) {
            self.current_position = position;
        } else {
            self.checkpoint_position = position;
        }
    }
}

impl std::fmt::Display for CdcResumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "checkpoint_position: {}, current_position: {}",
            json!(self.checkpoint_position),
            json!(self.current_position),
        )
    }
}
