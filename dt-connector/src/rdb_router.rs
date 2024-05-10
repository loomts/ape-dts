use std::collections::HashMap;

use dt_common::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser, router_config::RouterConfig,
    },
    error::Error,
    utils::sql_util::SqlUtil,
};

use dt_common::meta::{col_value::ColValue, row_data::RowData};
use serde::{Deserialize, Serialize};

type TbMap = HashMap<(String, String), (String, String)>;
type TbColMap = HashMap<(String, String), HashMap<String, String>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdbRouter {
    // HashMap<src_db, dst_db>
    pub db_map: HashMap<String, String>,
    // HashMap<(src_db, src_tb), (dst_db, dst_tb)>
    pub tb_map: TbMap,
    // HashMap<(src_db, src_tb), HashMap<src_col, dst_col>>
    pub tb_col_map: TbColMap,
    // HashMap<(src_db, src_tb), String>
    pub topic_map: HashMap<(String, String), String>,
}

impl RdbRouter {
    pub fn from_config(config: &RouterConfig, db_type: &DbType) -> Result<Self, Error> {
        match config {
            RouterConfig::Rdb {
                db_map,
                tb_map,
                col_map,
                topic_map,
            } => {
                let db_map = Self::parse_db_map(db_map, db_type).unwrap();
                let mut tb_map = Self::parse_tb_map(tb_map, db_type).unwrap();
                let (tb_map_2, tb_col_map) = Self::parse_tb_col_map(col_map, db_type).unwrap();
                for (k, v) in tb_map_2 {
                    tb_map.insert(k, v);
                }
                let topic_map = Self::parse_topic_map(topic_map, db_type).unwrap();
                Ok(Self {
                    db_map,
                    tb_map,
                    tb_col_map,
                    topic_map,
                })
            }
        }
    }

    pub fn get_tb_map<'a>(&'a self, db: &'a str, tb: &'a str) -> (&'a str, &'a str) {
        if let Some((dst_db, dst_tb)) = self.tb_map.get(&(db.into(), tb.into())) {
            return (dst_db, dst_tb);
        }
        if let Some(dst_db) = self.db_map.get(db) {
            return (dst_db, tb);
        }
        (db, tb)
    }

    pub fn get_col_map(&self, db: &str, tb: &str) -> Option<&HashMap<String, String>> {
        self.tb_col_map.get(&(db.into(), tb.into()))
    }

    pub fn get_topic<'a>(&'a self, db: &str, tb: &str) -> &'a str {
        // *.*:test,test_db_1.*:test2,test_db_1.no_pk_one_uk:test3
        if let Some(topic) = self.topic_map.get(&(db.into(), tb.into())) {
            return topic;
        }
        if let Some(topic) = self.topic_map.get(&(db.into(), "*".into())) {
            return topic;
        }
        // shoud always has a default topic map
        return self.topic_map.get(&("*".into(), "*".into())).unwrap();
    }

    pub fn reverse(&self) -> Self {
        let mut reverse_db_map = HashMap::new();
        let mut reverse_tb_map = HashMap::new();
        let mut reverse_tb_col_map = HashMap::new();

        for (src_db_tb, col_map) in self.tb_col_map.iter() {
            let mut reverse_col_map = HashMap::new();
            for (src_col, dst_col) in col_map.iter() {
                reverse_col_map.insert(dst_col.into(), src_col.into());
            }
            let dst_tb = self.tb_map.get(src_db_tb).unwrap();
            reverse_tb_col_map.insert(dst_tb.clone(), reverse_col_map);
        }

        for (src_tb, dst_tb) in self.tb_map.iter() {
            reverse_tb_map.insert(dst_tb.to_owned(), src_tb.to_owned());
        }

        for (src_db, dst_db) in self.db_map.iter() {
            reverse_db_map.insert(dst_db.to_owned(), src_db.to_owned());
        }

        Self {
            db_map: reverse_db_map,
            tb_map: reverse_tb_map,
            tb_col_map: reverse_tb_col_map,
            // topic_map should not be reversed
            topic_map: self.topic_map.clone(),
        }
    }

    pub fn route_row(&self, mut row_data: RowData) -> RowData {
        // tb map
        let (schema, tb) = (row_data.schema.clone(), row_data.tb.clone());
        let (dst_schema, dst_tb) = self.get_tb_map(&schema, &tb);
        row_data.schema = dst_schema.to_string();
        row_data.tb = dst_tb.to_string();

        // col map
        let col_map = self.get_col_map(&schema, &tb);
        if col_map.is_none() {
            return row_data;
        }
        let col_map = col_map.unwrap();

        let route_col_values =
            |col_values: HashMap<String, ColValue>| -> HashMap<String, ColValue> {
                let mut new_col_values = HashMap::new();
                for (col, col_value) in col_values {
                    if let Some(dst_col) = col_map.get(&col) {
                        new_col_values.insert(dst_col.to_owned(), col_value);
                    } else {
                        new_col_values.insert(col, col_value);
                    }
                }
                new_col_values
            };

        if let Some(before) = row_data.before {
            row_data.before = Some(route_col_values(before));
        }

        if let Some(after) = row_data.after {
            row_data.after = Some(route_col_values(after));
        }

        row_data
    }

    fn parse_db_map(config_str: &str, db_type: &DbType) -> Result<HashMap<String, String>, Error> {
        // db_map=src_db_1:dst_db_1,src_db_2:dst_db_2
        let mut db_map = HashMap::new();
        let tokens = Self::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            db_map.insert(tokens[i].to_string(), tokens[i + 1].to_string());
            i += 2;
        }
        Ok(db_map)
    }

    #[allow(clippy::type_complexity)]
    fn parse_tb_map(
        config_str: &str,
        db_type: &DbType,
    ) -> Result<HashMap<(String, String), (String, String)>, Error> {
        // tb_map=src_db_1.src_tb_1:dst_db_1.dst_tb_1,src_db_2.src_tb_2:dst_db_2.dst_tb_2
        let mut tb_map = HashMap::new();
        let tokens = Self::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            tb_map.insert(
                (tokens[i].to_string(), tokens[i + 1].to_string()),
                (tokens[i + 2].to_string(), tokens[i + 3].to_string()),
            );
            i += 4;
        }
        Ok(tb_map)
    }

    fn parse_tb_col_map(config_str: &str, db_type: &DbType) -> Result<(TbMap, TbColMap), Error> {
        // col_map=src_db_1.src_tb_1.col_1:dst_db_1.dst_tb_1.dst_col_1,src_db_2.src_tb_2.dst_col_2:dst_db_2.dst_tb_2.dst_col_2
        let mut tb_map = TbMap::new();
        let mut tb_col_map = TbColMap::new();

        let tokens = Self::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            let src_db_tb = (tokens[i].to_string(), tokens[i + 1].to_string());
            let dst_db_tb = (tokens[i + 3].to_string(), tokens[i + 4].to_string());
            let src_col = tokens[i + 2].to_string();
            let dst_col = tokens[i + 5].to_string();

            tb_map.insert(src_db_tb.clone(), dst_db_tb);
            if let Some(col_map) = tb_col_map.get_mut(&src_db_tb) {
                col_map.insert(src_col, dst_col);
            } else {
                let mut col_map = HashMap::new();
                col_map.insert(src_col, dst_col);
                tb_col_map.insert(src_db_tb, col_map);
            }
            i += 6;
        }

        Ok((tb_map, tb_col_map))
    }

    fn parse_topic_map(
        config_str: &str,
        db_type: &DbType,
    ) -> Result<HashMap<(String, String), String>, Error> {
        // topic_map=*.*:test,test_db_1.*:test2,test_db_1.no_pk_one_uk:test3
        let mut topic_map = HashMap::new();
        let tokens = Self::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            topic_map.insert(
                (tokens[i].to_string(), tokens[i + 1].to_string()),
                tokens[i + 2].to_string(),
            );
            i += 3;
        }
        Ok(topic_map)
    }

    fn parse_config(config_str: &str, db_type: &DbType) -> Result<Vec<String>, Error> {
        let delimiters = vec![',', '.', ':'];
        match ConfigTokenParser::parse_config(config_str, db_type, &delimiters) {
            Ok(tokens) => {
                let escape_pairs = SqlUtil::get_escape_pairs(db_type);
                let mut results = Vec::new();
                for t in tokens {
                    let mut token = t;
                    for escape_pair in escape_pairs.iter() {
                        token = SqlUtil::unescape(&token, escape_pair);
                    }
                    results.push(token);
                }
                Ok(results)
            }

            Err(Error::ConfigError(err)) => Err(Error::ConfigError(format!(
                "invalid router config, {}",
                err
            ))),

            _ => Err(Error::ConfigError("invalid router config".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dt_common::config::{config_enums::DbType, router_config::RouterConfig};

    use super::{RdbRouter, TbColMap, TbMap};

    #[test]
    fn test_parse_db_map() {
        // mysql
        let config_str = "src_1:dst_1,`src,2'`:dst_2,`src:3,`:`dst:3,`";
        let db_map = RdbRouter::parse_db_map(config_str, &DbType::Mysql).unwrap();
        assert_eq!(db_map.get("src_1").unwrap(), "dst_1");
        assert_eq!(db_map.get("src,2'").unwrap(), "dst_2");
        assert_eq!(db_map.get("src:3,").unwrap(), "dst:3,");
        assert_eq!(db_map.get("src_4"), None);

        // pg
        let config_str = r#"src_1:dst_1,"src,2'":dst_2,"src:3,":"dst:3,""#;
        let db_map = RdbRouter::parse_db_map(config_str, &DbType::Pg).unwrap();
        assert_eq!(db_map.get("src_1").unwrap(), "dst_1");
        assert_eq!(db_map.get("src,2'").unwrap(), "dst_2");
        assert_eq!(db_map.get("src:3,").unwrap(), "dst:3,");
        assert_eq!(db_map.get("src_4"), None);
    }

    #[test]
    fn test_parse_tb_map() {
        let assert_exists =
            |tb_map: &TbMap, src_db: &str, src_tb: &str, dst_db: &str, dst_tb: &str| {
                assert_eq!(
                    tb_map.get(&(src_db.into(), src_tb.into())).unwrap(),
                    &(dst_db.into(), dst_tb.into())
                )
            };

        // mysql
        let config_str = "src_db_1.src_tb_1:dst_db_1.dst_tb_1,".to_string()
            + "`src_db,2'`.`src_tb,2'`:dst_db_2.dst_tb_2,"
            + "`src_db:3,`.`src_tb:3,`:`dst_db:3,`.`dst_tb:3,`";
        let tb_map = RdbRouter::parse_tb_map(&config_str, &DbType::Mysql).unwrap();

        assert_exists(&tb_map, "src_db_1", "src_tb_1", "dst_db_1", "dst_tb_1");
        assert_exists(&tb_map, "src_db,2'", "src_tb,2'", "dst_db_2", "dst_tb_2");
        assert_exists(&tb_map, "src_db:3,", "src_tb:3,", "dst_db:3,", "dst_tb:3,");
        assert_eq!(tb_map.get(&("src_db_4".into(), "src_tb_4".into())), None);

        // pg
        let config_str = r#"src_db_1.src_tb_1:dst_db_1.dst_tb_1,"#.to_string()
            + r#""src_db,2'"."src_tb,2'":dst_db_2.dst_tb_2,"#
            + r#""src_db:3,"."src_tb:3,":"dst_db:3,"."dst_tb:3,""#;
        let tb_map = RdbRouter::parse_tb_map(&config_str, &DbType::Pg).unwrap();

        assert_exists(&tb_map, "src_db_1", "src_tb_1", "dst_db_1", "dst_tb_1");
        assert_exists(&tb_map, "src_db,2'", "src_tb,2'", "dst_db_2", "dst_tb_2");
        assert_exists(&tb_map, "src_db:3,", "src_tb:3,", "dst_db:3,", "dst_tb:3,");
        assert_eq!(tb_map.get(&("src_db_4".into(), "src_tb_4".into())), None);
    }

    #[test]
    fn test_parse_col_map() {
        let assert_tb_map =
            |tb_map: &TbMap, src_db: &str, src_tb: &str, dst_db: &str, dst_tb: &str| {
                assert_eq!(
                    tb_map.get(&(src_db.into(), src_tb.into())).unwrap(),
                    &(dst_db.into(), dst_tb.into())
                )
            };

        let assert_col_map =
            |tb_map: &TbColMap, src_db: &str, src_tb: &str, col_map: &HashMap<String, String>| {
                assert_eq!(
                    tb_map.get(&(src_db.into(), src_tb.into())).unwrap(),
                    col_map
                )
            };

        let check_results = |tb_map: &TbMap, tb_col_map: &TbColMap| {
            assert_tb_map(&tb_map, "src_db_1", "src_tb_1", "dst_db_1", "dst_tb_1");
            assert_tb_map(&tb_map, "src_db,2'", "src_tb,2'", "dst_db_2", "dst_tb_2");
            assert_tb_map(&tb_map, "src_db:3,", "src_tb:3,", "dst_db:3,", "dst_tb:3,");
            assert_eq!(tb_map.get(&("src_db_4".into(), "src_tb_4".into())), None);

            let mut col_map = HashMap::new();
            col_map.insert("src_col_1".to_string(), "dst_col_1".to_string());
            col_map.insert("src_col_2".to_string(), "dst_col_2".to_string());
            assert_col_map(&tb_col_map, "src_db_1", "src_tb_1", &col_map);

            let mut col_map = HashMap::new();
            col_map.insert("src_col,1'".to_string(), "dst_col_1".to_string());
            col_map.insert("src_col,2'".to_string(), "dst_col_2".to_string());
            assert_col_map(&tb_col_map, "src_db,2'", "src_tb,2'", &col_map);

            let mut col_map = HashMap::new();
            col_map.insert("src_col:1,".to_string(), "dst_col:1,".to_string());
            col_map.insert("src_col:2,".to_string(), "dst_col:2,".to_string());
            assert_col_map(&tb_col_map, "src_db:3,", "src_tb:3,", &col_map);

            assert_eq!(
                tb_col_map.get(&("src_db_4".into(), "src_tb_4".into())),
                None
            );
        };

        // mysql
        let config_str = "src_db_1.src_tb_1.src_col_1:dst_db_1.dst_tb_1.dst_col_1,".to_string()
            + "src_db_1.src_tb_1.src_col_2:dst_db_1.dst_tb_1.dst_col_2,"
            + "`src_db,2'`.`src_tb,2'`.`src_col,1'`:dst_db_2.dst_tb_2.dst_col_1,"
            + "`src_db,2'`.`src_tb,2'`.`src_col,2'`:dst_db_2.dst_tb_2.dst_col_2,"
            + "`src_db:3,`.`src_tb:3,`.`src_col:1,`:`dst_db:3,`.`dst_tb:3,`.`dst_col:1,`,"
            + "`src_db:3,`.`src_tb:3,`.`src_col:2,`:`dst_db:3,`.`dst_tb:3,`.`dst_col:2,`";
        let (tb_map, tb_col_map) =
            RdbRouter::parse_tb_col_map(&config_str, &DbType::Mysql).unwrap();
        check_results(&tb_map, &tb_col_map);

        // pg
        let config_str = r#"src_db_1.src_tb_1.src_col_1:dst_db_1.dst_tb_1.dst_col_1,"#.to_string()
            + r#"src_db_1.src_tb_1.src_col_2:dst_db_1.dst_tb_1.dst_col_2,"#
            + r#""src_db,2'"."src_tb,2'"."src_col,1'":dst_db_2.dst_tb_2.dst_col_1,"#
            + r#""src_db,2'"."src_tb,2'"."src_col,2'":dst_db_2.dst_tb_2.dst_col_2,"#
            + r#""src_db:3,"."src_tb:3,"."src_col:1,":"dst_db:3,"."dst_tb:3,"."dst_col:1,","#
            + r#""src_db:3,"."src_tb:3,"."src_col:2,":"dst_db:3,"."dst_tb:3,"."dst_col:2,""#;
        let (tb_map, tb_col_map) = RdbRouter::parse_tb_col_map(&config_str, &DbType::Pg).unwrap();
        check_results(&tb_map, &tb_col_map);
    }

    #[test]
    fn test_parse_config() {
        let db_map_str = "src_1:dst_1";
        let tb_map_str = "`src_db,2'`.`src_tb,2'`:dst_db_2.dst_tb_2";
        let field_map_str =
            "`src_db:3,`.`src_tb:3,`.`src_col:1,`:`dst_db:3,`.`dst_tb:3,`.`dst_col:1,`,"
                .to_string()
                + "`src_db:3,`.`src_tb:3,`.`src_col:2,`:`dst_db:3,`.`dst_tb:3,`.`dst_col:2,`";
        let topic_map = "*.*:test,`db:1`.*:test2,`db:1`.`tb:1`:test3";

        let config = RouterConfig::Rdb {
            db_map: db_map_str.into(),
            tb_map: tb_map_str.into(),
            col_map: field_map_str.into(),
            topic_map: topic_map.into(),
        };
        let router = RdbRouter::from_config(&config, &DbType::Mysql).unwrap();

        let assert_tb_map = |src_db: &str, src_tb: &str, dst_db: &str, dst_tb: &str| {
            assert_eq!(router.get_tb_map(src_db, src_tb), (dst_db, dst_tb));
        };
        let assert_col_map = |src_db: &str, src_tb: &str, col_map: &HashMap<String, String>| {
            assert_eq!(router.get_col_map(src_db, src_tb).unwrap(), col_map)
        };

        // db_map
        assert_tb_map("src_1", "aaa.1,:1", "dst_1", "aaa.1,:1");
        assert_tb_map("src_4", "aaa.1,:1", "src_4", "aaa.1,:1");
        // tb_map
        assert_tb_map("src_db,2'", "src_tb,2'", "dst_db_2", "dst_tb_2");
        assert_tb_map("src_db,2'", "src_tb,3'", "src_db,2'", "src_tb,3'");
        // col_map
        let mut col_map = HashMap::new();
        col_map.insert("src_col:1,".to_string(), "dst_col:1,".to_string());
        col_map.insert("src_col:2,".to_string(), "dst_col:2,".to_string());
        assert_col_map("src_db:3,", "src_tb:3,", &col_map);
        // topic_map
        assert_eq!(router.get_topic("db:1", "tb:1"), "test3");
        assert_eq!(router.get_topic("db:1", "tb:2"), "test2");
        assert_eq!(router.get_topic("db:2", "tb:1"), "test");
    }
}
