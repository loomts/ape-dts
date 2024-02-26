use std::collections::HashMap;

use dt_common::{
    config::{config_enums::DbType, resumer_config::ResumerConfig},
    error::Error,
    utils::sql_util::SqlUtil,
};

#[derive(Clone)]
pub struct SnapshotResumer {
    resume_values: HashMap<String, String>,
    db_type: DbType,
}

const RDB_SNAPSHOT_POSITIONS: &str = "rdb_snapshot_positions";

impl SnapshotResumer {
    pub fn new(db_type: &DbType, config: &ResumerConfig) -> Result<Self, Error> {
        let mut resume_values: HashMap<String, String> = HashMap::new();
        if let Some(Some(positions)) = config.resume_values.get(RDB_SNAPSHOT_POSITIONS) {
            resume_values = serde_json::from_str(positions).unwrap();
        }
        Ok(Self {
            resume_values,
            db_type: db_type.clone(),
        })
    }

    pub fn get_resume_value(&self, db: &str, tb: &str, col: &str) -> Option<String> {
        if !SqlUtil::get_escape_pairs(&self.db_type).is_empty() {
            let tokens = vec![db.to_string(), tb.to_string(), col.to_string()];
            let escaped_tokens = tokens
                .iter()
                .map(|i| SqlUtil::escape_by_db_type(i, &self.db_type))
                .collect::<Vec<String>>();

            let combinations = Self::get_combinations(&tokens[0..], &escaped_tokens[0..]);
            for i in combinations.iter() {
                if let Some(value) = self
                    .resume_values
                    .get(&format!("{}.{}.{}", i[0], i[1], i[2]))
                {
                    return Some(value.clone());
                }
            }
        } else if let Some(value) = self.resume_values.get(&format!("{}.{}.{}", db, tb, col)) {
            return Some(value.clone());
        }
        None
    }

    fn get_combinations(a: &[String], b: &[String]) -> Vec<Vec<String>> {
        if a.is_empty() || a.len() != b.len() {
            return vec![];
        }

        if a.len() == 1 {
            return vec![vec![a[0].clone()], vec![b[0].clone()]];
        }

        let sub_results = Self::get_combinations(&a[1..], &b[1..]);
        let mut results = Vec::new();
        for sub in sub_results.iter() {
            let mut result_1 = vec![a[0].clone()];
            result_1.extend(sub.clone());
            results.push(result_1);

            let mut result_2 = vec![b[0].clone()];
            result_2.extend(sub.clone());
            results.push(result_2)
        }
        results
    }
}

#[cfg(test)]
mod tests {

    use serde_json::json;

    use super::*;

    #[test]
    fn test_get_resume_value_1() {
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
        ];
        let not_exist_expects = vec![("d6", "t6", "c6")];
        check_resume_values(
            DbType::Mysql,
            &position_str,
            &exist_expects,
            &not_exist_expects,
        );

        // check for pg
        // the escape character for mysql is "
        let exist_expects = vec![("d0", "t0", "c0", "v0"), ("d6", "t6", "c6", "v6")];
        let not_exist_expects = vec![
            ("d1", "t1", "c1"),
            ("d2", "t2", "c2"),
            ("d3", "t3", "c3"),
            ("d4", "t4", "c4"),
            ("d5", "t5", "c5"),
        ];
        check_resume_values(
            DbType::Pg,
            &position_str,
            &exist_expects,
            &not_exist_expects,
        );
    }

    #[test]
    fn test_get_resume_value_2() {
        let positions = "{\"\\\"db_3_,\\\".\\\"tb_3_*$\\\".\\\"p.k\\\"\":\"\\\"bbb\\\"\\\"\\\"\",\"`db_2_,`.`tb_2_*$`.`p.k`\":\"`aaa```\",\"db_0.tb_0.f_0\":\"0\"}";
        // map[`db_0.tb_0.f_0`] = `0`
        // map["`db_2_,`.`tb_2_*$`.`p.k`"] = "`aaa```"
        // map["db_3_,"."tb_3_*$"."p.k"] = `"bbb"""`

        // check for mysql
        let exist_expects = vec![
            ("db_0", "tb_0", "f_0", "0"),
            ("db_2_,", "tb_2_*$", "p.k", "`aaa```"),
            ("`db_2_,`", "`tb_2_*$`", "`p.k`", "`aaa```"),
            (r#""db_3_,""#, r#""tb_3_*$""#, r#""p.k""#, r#""bbb""""#),
        ];
        let not_exist_expects = vec![("db_3_,", "tb_3_*$", "p.k")];
        check_resume_values(DbType::Mysql, positions, &exist_expects, &not_exist_expects);

        // check for pg
        let exist_expects = vec![
            ("db_0", "tb_0", "f_0", "0"),
            ("`db_2_,`", "`tb_2_*$`", "`p.k`", "`aaa```"),
            ("db_3_,", "tb_3_*$", "p.k", r#""bbb""""#),
            (r#""db_3_,""#, r#""tb_3_*$""#, r#""p.k""#, r#""bbb""""#),
        ];
        let not_exist_expects = vec![("db_2_,", "tb_2_*$", "p.k")];
        check_resume_values(DbType::Pg, positions, &exist_expects, &not_exist_expects);
    }

    fn check_resume_values(
        db_type: DbType,
        positions: &str,
        exist_expects: &Vec<(&str, &str, &str, &str)>,
        not_exist_expects: &Vec<(&str, &str, &str)>,
    ) {
        let mut resume_values = HashMap::new();
        resume_values.insert(
            RDB_SNAPSHOT_POSITIONS.to_string(),
            Some(positions.to_string()),
        );
        let resumer_config = ResumerConfig { resume_values };
        let resumer = SnapshotResumer::new(&db_type, &resumer_config).unwrap();

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
