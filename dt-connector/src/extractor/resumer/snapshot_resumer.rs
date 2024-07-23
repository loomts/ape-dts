use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
};

use dt_common::{config::resumer_config::ResumerConfig, utils::file_util::FileUtil};
use dt_common::{log_info, meta::position::Position};

#[derive(Clone)]
pub struct SnapshotResumer {
    tb_positions: HashMap<DbTbCol, String>,
    finished_tbs: HashSet<DbTb>,
}

type DbTbCol = (String, String, String);
type DbTb = (String, String);

const TAIL_POSITION_COUNT: usize = 30;

impl SnapshotResumer {
    pub fn from_config(config: &ResumerConfig) -> anyhow::Result<Self> {
        let mut tb_positions: HashMap<DbTbCol, String> = HashMap::new();
        let mut finished_tbs: HashSet<DbTb> = HashSet::new();

        if let Ok(file) = File::open(&config.resume_config_file) {
            for line in BufReader::new(file).lines().map_while(Result::ok) {
                Self::load_resume_line(&mut tb_positions, &mut finished_tbs, &line)
            }
        }

        if config.resume_from_log {
            let position_log = format!("{}/position.log", config.resume_log_dir);
            // currently we only need the last line in position.log
            // since only 1 table is being processed at the same time
            if let Ok(lines) = FileUtil::tail(&position_log, TAIL_POSITION_COUNT) {
                for line in lines.iter() {
                    Self::load_resume_line(&mut tb_positions, &mut finished_tbs, &line)
                }
            }

            let finished_log = format!("{}/finished.log", config.resume_log_dir);
            if let Ok(file) = File::open(finished_log) {
                for line in BufReader::new(file).lines().map_while(Result::ok) {
                    Self::load_resume_line(&mut tb_positions, &mut finished_tbs, &line)
                }
            }
        }

        Ok(Self {
            tb_positions,
            finished_tbs,
        })
    }

    pub fn check_finished(&self, db: &str, tb: &str) -> bool {
        let res = self
            .finished_tbs
            .contains(&(db.to_string(), tb.to_string()));
        log_info!(
            "resumer, check finished: db: {}, tb: {}, result: {}",
            db,
            tb,
            res
        );
        res
    }

    pub fn get_resume_value(&self, db: &str, tb: &str, col: &str) -> Option<String> {
        let mut res = None;
        if let Some(value) =
            self.tb_positions
                .get(&(db.to_string(), tb.to_string(), col.to_string()))
        {
            res = Some(value.clone());
        }

        log_info!(
            "resumer, get resume value, db: {}, tb: {}, col: {}, result: {:?}",
            db,
            tb,
            col,
            res
        );
        res
    }

    fn load_resume_line(
        tb_positions: &mut HashMap<DbTbCol, String>,
        finished_tbs: &mut HashSet<DbTb>,
        line: &str,
    ) {
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
                finished_tbs.insert((schema, tb));
            }

            _ => {}
        }
    }
}
