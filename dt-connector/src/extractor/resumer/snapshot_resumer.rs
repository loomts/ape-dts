use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
};

use dt_common::{config::resumer_config::ResumerConfig, utils::file_util::FileUtil};
use dt_common::{log_info, meta::position::Position};

use super::{CURRENT_POSITION_LOG_FLAG, TAIL_POSITION_COUNT};

#[derive(Clone, Default)]
pub struct SnapshotResumer {
    current_tb_positions: HashMap<DbTbCol, String>,
    checkpoint_tb_positions: HashMap<DbTbCol, String>,
    finished_tbs: HashSet<DbTb>,
}

type DbTbCol = (String, String, String);
type DbTb = (String, String);

impl SnapshotResumer {
    pub fn from_config(config: &ResumerConfig) -> anyhow::Result<Self> {
        let mut me = Self::default();

        if let Ok(file) = File::open(&config.resume_config_file) {
            for line in BufReader::new(file).lines().map_while(Result::ok) {
                me.load_resume_line(&line)
            }
        }

        if config.resume_from_log {
            let position_log = format!("{}/position.log", config.resume_log_dir);
            // currently we only need the last line in position.log
            // since only 1 table is being processed at the same time
            if let Ok(lines) = FileUtil::tail(&position_log, TAIL_POSITION_COUNT) {
                for line in lines.iter() {
                    me.load_resume_line(line)
                }
            }

            let finished_log = format!("{}/finished.log", config.resume_log_dir);
            if let Ok(file) = File::open(finished_log) {
                for line in BufReader::new(file).lines().map_while(Result::ok) {
                    me.load_resume_line(&line)
                }
            }
        }
        Ok(me)
    }

    pub fn check_finished(&self, schema: &str, tb: &str) -> bool {
        let res = self
            .finished_tbs
            .contains(&(schema.to_string(), tb.to_string()));
        log_info!(
            "resumer, check finished: schema: {}, tb: {}, result: {}",
            schema,
            tb,
            res
        );
        res
    }

    pub fn get_resume_value(
        &self,
        schema: &str,
        tb: &str,
        col: &str,
        checkpoint: bool,
    ) -> Option<String> {
        let key = (schema.to_string(), tb.to_string(), col.to_string());
        let tb_positions = if !checkpoint && self.current_tb_positions.contains_key(&key) {
            &self.current_tb_positions
        } else {
            &self.checkpoint_tb_positions
        };

        let mut res = None;
        if let Some(value) = tb_positions.get(&key) {
            res = Some(value.clone());
        }

        log_info!(
            "resumer, get resume value, schema: {}, tb: {}, col: {}, result: {:?}",
            schema,
            tb,
            col,
            res
        );
        res
    }

    fn load_resume_line(&mut self, line: &str) {
        // by default, all positions in resumer.config are checkpoint positions
        let tb_positions = if line.contains(CURRENT_POSITION_LOG_FLAG) {
            &mut self.current_tb_positions
        } else {
            &mut self.checkpoint_tb_positions
        };

        match Position::from_log(line) {
            Position::RdbSnapshot {
                schema,
                tb,
                order_col,
                value,
                ..
            } => {
                tb_positions.insert((schema, tb, order_col), value);
            }

            Position::FoxlakeS3 {
                schema,
                tb,
                s3_meta_file,
            } => {
                tb_positions.insert((schema, tb, String::new()), s3_meta_file);
            }

            Position::RdbSnapshotFinished { schema, tb, .. } => {
                self.finished_tbs.insert((schema, tb));
            }

            _ => {}
        }
    }
}
