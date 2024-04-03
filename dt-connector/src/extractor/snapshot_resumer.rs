use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};

use dt_common::meta::position::Position;
use dt_common::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser, resumer_config::ResumerConfig,
    },
    error::Error,
    utils::{file_util::FileUtil, sql_util::SqlUtil},
};

#[derive(Clone)]
pub struct SnapshotResumer {
    tb_positions: HashMap<DbTbCol, String>,
    finished_tbs: HashSet<DbTb>,
}

type DbTbCol = (String, String, String);
type DbTb = (String, String);

const TAIL_POSITION_COUNT: usize = 30;

impl SnapshotResumer {
    pub fn from_config(config: &ResumerConfig, db_type: &DbType) -> Result<Self, Error> {
        let delimiters = ['.', ','];

        // tb_positions={"`d4`.`t4`.c4":"v4","d3.t3.`c3`":"v3"}
        let mut tb_positions: HashMap<DbTbCol, String> = HashMap::new();
        if !config.tb_positions.is_empty() {
            let raw_tb_positions: HashMap<String, String> =
                serde_json::from_str(&config.tb_positions).unwrap();
            for (db_tb_col, value) in raw_tb_positions {
                let tokens = ConfigTokenParser::parse_config(&db_tb_col, db_type, &['.'])
                    .expect("error config: [resumer]tb_positions");
                let db = SqlUtil::unescape_by_db_type(&tokens[0], db_type);
                let tb = SqlUtil::unescape_by_db_type(&tokens[1], db_type);
                let col = SqlUtil::unescape_by_db_type(&tokens[2], db_type);
                tb_positions.insert((db, tb, col), value);
            }
        }

        // finished_tbs=ab.cd,`a.b`.cd,`a.b`.`c.d`
        let mut finished_tbs: HashSet<DbTb> = HashSet::new();
        if !config.finished_tbs.is_empty() {
            let tokens =
                ConfigTokenParser::parse_config(&config.finished_tbs, db_type, &delimiters)
                    .expect("error config: [resumer]finished_tbs");
            let mut i = 0;
            while i < tokens.len() {
                let db = SqlUtil::unescape_by_db_type(&tokens[i], db_type);
                let tb = SqlUtil::unescape_by_db_type(&tokens[i + 1], db_type);
                finished_tbs.insert((db, tb));
                i += 2;
            }
        }

        if config.resume_from_log {
            let position_log = format!("{}/position.log", config.resume_log_dir);
            // currently we only need the last line in position.log
            // since only 1 table is being processed at the same time
            if let Ok(lines) = FileUtil::tail(&position_log, TAIL_POSITION_COUNT) {
                for line in lines.iter() {
                    if let Position::RdbSnapshot {
                        schema,
                        tb,
                        order_col,
                        value,
                        ..
                    } = Self::get_position_from_log(line)
                    {
                        tb_positions.insert((schema, tb, order_col), value);
                    }
                }
            }

            let finished_log = format!("{}/finished.log", config.resume_log_dir);
            if let Ok(file) = File::open(&finished_log) {
                for line in BufReader::new(file).lines().flatten() {
                    if let Position::RdbSnapshotFinished { schema, tb, .. } =
                        Self::get_position_from_log(&line)
                    {
                        finished_tbs.insert((schema, tb));
                    }
                }
            }
        }

        Ok(Self {
            tb_positions,
            finished_tbs,
        })
    }

    pub fn check_finished(&self, db: &str, tb: &str) -> bool {
        self.finished_tbs
            .contains(&(db.to_string(), tb.to_string()))
    }

    pub fn get_resume_value(&self, db: &str, tb: &str, col: &str) -> Option<String> {
        if let Some(value) =
            self.tb_positions
                .get(&(db.to_string(), tb.to_string(), col.to_string()))
        {
            return Some(value.clone());
        }
        None
    }

    fn get_position_from_log(log: &str) -> Position {
        // 2024-03-29 07:02:24.463776 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col":"f_0","value":"9"}
        // 2024-04-01 03:25:18.701725 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk"}
        if log.trim().is_empty() {
            return Position::None;
        }

        let error = format!("invalid position log: {}", log);
        let left = log.find('{').expect(&error);
        let right = log.rfind('}').expect(&error);
        let position_log = &log[left..=right];
        Position::from_str(position_log).expect(&error)
    }
}

#[cfg(test)]
mod tests {

    use serde_json::json;

    use super::*;

    #[test]
    fn test_get_position_from_log() {
        let log1 = r#"2024-04-01 03:25:18.701725 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk"}"#;
        let log2 = r#"2024-03-29 07:02:24.463776 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col":"f_0","value":"9"}"#;

        if let Position::RdbSnapshotFinished {
            db_type,
            schema,
            tb,
        } = SnapshotResumer::get_position_from_log(log1)
        {
            assert_eq!(db_type, "mysql");
            assert_eq!(schema, "test_db_1");
            assert_eq!(tb, "one_pk_no_uk");
        } else {
            assert!(false)
        }

        if let Position::RdbSnapshot {
            db_type,
            schema,
            tb,
            order_col,
            value,
        } = SnapshotResumer::get_position_from_log(log2)
        {
            assert_eq!(db_type, "mysql");
            assert_eq!(schema, "test_db_1");
            assert_eq!(tb, "one_pk_no_uk");
            assert_eq!(order_col, "f_0");
            assert_eq!(value, "9");
        } else {
            assert!(false)
        }
    }

    #[test]
    fn test_finished() {
        let finished_tbs = "ab.cd,`a.b`.cd,`a.b`.`c.d`,`\"a.b\"`.`\"c.d\"`".to_string();
        let mut resumer_config = ResumerConfig {
            finished_tbs,
            ..Default::default()
        };
        let resumer = SnapshotResumer::from_config(&resumer_config, &DbType::Mysql).unwrap();
        assert!(resumer.check_finished("ab", "cd"));
        assert!(resumer.check_finished("a.b", "cd"));
        assert!(resumer.check_finished("a.b", "c.d"));
        assert!(resumer.check_finished("\"a.b\"", "\"c.d\""));

        resumer_config.finished_tbs =
            "ab.cd,\"a.b\".cd,\"a.b\".\"c.d\",\"`a.b`\".\"`c.d`\"".to_string();
        let resumer = SnapshotResumer::from_config(&resumer_config, &DbType::Pg).unwrap();
        assert!(resumer.check_finished("ab", "cd"));
        assert!(resumer.check_finished("a.b", "cd"));
        assert!(resumer.check_finished("a.b", "c.d"));
        assert!(resumer.check_finished("`a.b`", "`c.d`"));
    }

    #[test]
    fn test_get_position() {
        let mut positions = HashMap::new();
        let mut push_in_positions = |k: &str, v: &str| {
            positions.insert(k.to_string(), v.to_string());
        };

        push_in_positions("d0.t0.c0", "v0");
        push_in_positions("`d1`.t1.c1", "v1");
        push_in_positions("d2.`t2`.c2", "v2");
        push_in_positions("d3.t3.`c3`", "v3");
        push_in_positions("`d4`.`t4`.c4", "v4");
        push_in_positions("`d5`.`t5`.`c5`", "v5");
        push_in_positions(r#""d6"."t6"."c6""#, "v6");

        let position_str = json!(positions).to_string();

        // check for mysql,
        // the escape character for mysql is `
        let exist_expects = vec![
            ("d0", "t0", "c0", "v0"),
            ("d1", "t1", "c1", "v1"),
            ("d2", "t2", "c2", "v2"),
            ("d3", "t3", "c3", "v3"),
            ("d4", "t4", "c4", "v4"),
            ("d5", "t5", "c5", "v5"),
            ("\"d6\"", "\"t6\"", "\"c6\"", "v6"),
        ];
        let not_exist_expects = vec![
            ("`d1`", "t1", "c1"),
            ("d2", "`t2`", "c2"),
            ("d3", "t3", "`c3`"),
            ("`d4`", "`t4`", "c4"),
            ("`d5`", "`t5`", "`c5`"),
            ("d6", "t6", "c6"),
        ];
        check_position(
            DbType::Mysql,
            &position_str,
            &exist_expects,
            &not_exist_expects,
        );

        // check for pg
        // the escape character for pg is "
        let exist_expects = vec![
            ("d0", "t0", "c0", "v0"),
            ("`d1`", "t1", "c1", "v1"),
            ("d2", "`t2`", "c2", "v2"),
            ("d3", "t3", "`c3`", "v3"),
            ("`d4`", "`t4`", "c4", "v4"),
            ("`d5`", "`t5`", "`c5`", "v5"),
            ("d6", "t6", "c6", "v6"),
        ];
        let not_exist_expects = vec![
            ("d1", "t1", "c1"),
            ("d2", "t2", "c2"),
            ("d3", "t3", "c3"),
            ("d4", "t4", "c4"),
            ("d5", "t5", "c5"),
            ("\"d6\"", "\"t6\"", "\"c6\""),
        ];
        check_position(
            DbType::Pg,
            &position_str,
            &exist_expects,
            &not_exist_expects,
        );
    }

    fn check_position(
        db_type: DbType,
        tb_positions: &str,
        exist_expects: &Vec<(&str, &str, &str, &str)>,
        not_exist_expects: &Vec<(&str, &str, &str)>,
    ) {
        let resumer_config = ResumerConfig {
            tb_positions: tb_positions.into(),
            ..Default::default()
        };
        let resumer = SnapshotResumer::from_config(&resumer_config, &db_type).unwrap();

        let assert_exists = |db: &str, tb: &str, col: &str, expected: &str| {
            assert_eq!(
                resumer.get_resume_value(db, tb, col),
                Some(expected.to_string())
            )
        };
        let assert_not_exists =
            |db: &str, tb: &str, col: &str| assert_eq!(resumer.get_resume_value(db, tb, col), None);

        for (db, tb, col, expected_value) in exist_expects {
            assert_exists(db, tb, col, expected_value);
        }

        for (db, tb, col) in not_exist_expects {
            assert_not_exists(db, tb, col);
        }
    }
}
