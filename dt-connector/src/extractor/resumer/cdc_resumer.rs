use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use anyhow::Context;
use dt_common::{
    config::{config_enums::ExtractType, task_config::TaskConfig},
    log_warn,
    meta::position::Position,
    utils::file_util::FileUtil,
};
use serde_json::json;

use super::{CURRENT_POSITION_LOG_FLAG, TAIL_POSITION_COUNT};

#[derive(Clone, Default)]
pub struct CdcResumer {
    pub current_position: Position,
    pub checkpoint_position: Position,
}

impl CdcResumer {
    pub fn from_config(task_config: &TaskConfig) -> anyhow::Result<Self> {
        let mut me = Self::default();
        if !matches!(task_config.extractor_basic.extract_type, ExtractType::Cdc) {
            return Ok(me);
        }

        let config = &task_config.resumer;
        if !config.resume_config_file.is_empty() {
            if Path::new(&config.resume_config_file).exists() {
                let file = File::open(&config.resume_config_file).with_context(|| {
                    format!(
                        "failed to open resume_config_file: [{}] while it exists",
                        config.resume_config_file
                    )
                })?;
                for line in BufReader::new(file).lines().map_while(Result::ok) {
                    me.load_resume_line(&line);
                }
                me.current_position = me.checkpoint_position.clone();
            } else {
                log_warn!(
                    "resume_config_file [{}] does not exist",
                    config.resume_config_file
                );
            }
        }

        if config.resume_from_log {
            let position_log = format!("{}/position.log", config.resume_log_dir);
            if Path::new(&position_log).exists() {
                let lines =
                    FileUtil::tail(&position_log, TAIL_POSITION_COUNT).with_context(|| {
                        format!(
                            "failed to open position.log: [{}] while it exists",
                            position_log
                        )
                    })?;
                for line in lines.iter() {
                    me.load_resume_line(line)
                }
            } else {
                log_warn!(
                    "resume_from_log is true, but [{}] does not exist",
                    position_log
                );
            }
        }
        Ok(me)
    }

    fn load_resume_line(&mut self, line: &str) {
        let position = Position::from_log(line);
        // ignore position log lines like:
        // 2025-02-18 04:13:04.655541 | checkpoint_position | {"type":"None"}
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
