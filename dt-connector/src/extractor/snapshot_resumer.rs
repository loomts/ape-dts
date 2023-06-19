use std::collections::HashMap;

use dt_common::{config::config_enums::DbType, utils::sql_util::SqlUtil};

#[derive(Clone)]
pub struct SnapshotResumer {
    pub resumer_values: HashMap<String, Option<String>>,
    pub db_type: DbType,
}

impl SnapshotResumer {
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
                    .resumer_values
                    .get(&format!("{}.{}.{}", i[0], i[1], i[2]))
                {
                    return value.clone();
                }
            }
        } else if let Some(value) = self.resumer_values.get(&format!("{}.{}.{}", db, tb, col)) {
            return value.clone();
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

    use super::*;

    #[test]
    fn test_get_resume_value() {
        let mut resumer_config = HashMap::new();

        let mut push_in_resumer_config = |k: &str, v: &str| {
            resumer_config.insert(k.to_string(), Some(v.to_string()));
        };

        push_in_resumer_config("d0.t0.c0", "v0");
        push_in_resumer_config("`d1`.t1.c1", "v1");
        push_in_resumer_config("d2.`t2`.c2", "v2");
        push_in_resumer_config("d3.t3.`c3`", "v3");
        push_in_resumer_config("`d4`.`t4`.c4", "v4");
        push_in_resumer_config("`d5`.`t5`.`c5`", "v5");
        resumer_config.insert("d6.t6.c6".to_string(), None);

        let resumer = SnapshotResumer {
            resumer_values: resumer_config,
            db_type: DbType::Mysql,
        };
        let assert_exists = |db: &str, tb: &str, col: &str, expected: &str| {
            assert_eq!(
                resumer.get_resume_value(db, tb, col),
                Some(expected.to_string())
            )
        };
        let assert_not_exists =
            |db: &str, tb: &str, col: &str| assert_eq!(resumer.get_resume_value(db, tb, col), None);

        assert_exists("d0", "t0", "c0", "v0");
        assert_exists("d1", "t1", "c1", "v1");
        assert_exists("d2", "t2", "c2", "v2");
        assert_exists("d3", "t3", "c3", "v3");
        assert_exists("d4", "t4", "c4", "v4");
        assert_exists("d5", "t5", "c5", "v5");

        assert_not_exists("d6", "t6", "c6");
        assert_not_exists("d7", "t7", "c7");
    }
}
