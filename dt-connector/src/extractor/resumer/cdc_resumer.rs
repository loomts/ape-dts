use dt_common::{
    config::resumer_config::ResumerConfig, error::Error, log_info, meta::position::Position,
    utils::file_util::FileUtil,
};

const TAIL_POSITION_COUNT: usize = 30;

#[derive(Clone)]
pub struct CdcResumer {
    pub position: Position,
}

impl CdcResumer {
    pub fn from_config(config: &ResumerConfig) -> Result<Self, Error> {
        let mut position = Position::None;
        if !config.resume_from_log {
            log_info!("cdc resume from log disabled");
            return Ok(Self { position });
        }

        let position_log = format!("{}/position.log", config.resume_log_dir);
        if let Ok(lines) = FileUtil::tail(&position_log, TAIL_POSITION_COUNT) {
            for line in lines.iter().rev() {
                // always use the last checkpoint_position if exists
                if line.contains("checkpoint_position") {
                    position = Position::from_log(line);
                    break;
                }

                // use the last current_position if no checkpoint_position exists
                if line.contains("current_position") && position == Position::None {
                    position = Position::from_log(line);
                }
            }
        }

        log_info!("cdc resume config: {}", position.to_string());
        Ok(Self { position })
    }
}
