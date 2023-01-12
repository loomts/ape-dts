use std::collections::HashMap;

use futures::TryStreamExt;

use sqlx::{mysql::MySqlRow, MySql, Pool, Row};

use crate::error::Error;

use super::{col_meta::ColMeta, col_type::ColType, tb_meta::TbMeta};

pub struct DbMetaManager<'a> {
    pub conn_pool: &'a Pool<MySql>,
}

impl DbMetaManager<'_> {
    pub async fn get_tb_meta(&self, db: &str, tb: &str) -> Result<TbMeta, Error> {
        let (cols, col_meta_map) = self.parse_cols(db, tb).await?;
        let key_map = self.parse_keys(db, tb).await?;
        let order_col = Self::get_order_col(&key_map)?;
        let where_cols = Self::get_where_cols(&key_map, &cols)?;
        Ok(TbMeta {
            db: db.to_string(),
            tb: tb.to_string(),
            cols,
            col_meta_map,
            key_map,
            order_col,
            where_cols,
        })
    }

    async fn parse_cols(
        &self,
        db: &str,
        tb: &str,
    ) -> Result<(Vec<String>, HashMap<String, ColMeta>), Error> {
        let mut cols = Vec::new();
        let mut col_meta_map = HashMap::new();

        let sql = format!("DESC {}.{}", db, tb);
        let mut rows = sqlx::query(&sql).fetch(self.conn_pool);
        while let Some(row) = rows.try_next().await? {
            let col_type = Self::parse_col_meta(&row)?;
            cols.push(col_type.name.clone());
            col_meta_map.insert(col_type.name.clone(), col_type);
        }

        if cols.is_empty() {
            return Err(Error::MetadataError {
                error: format!("failed to get table metadata for: {}.{}", db, tb),
            });
        }
        Ok((cols, col_meta_map))
    }

    fn parse_col_meta(row: &MySqlRow) -> Result<ColMeta, Error> {
        let col_name: String = row.try_get(0)?;
        let mut col_type_raw: String = row.try_get(1)?;
        col_type_raw = col_type_raw.to_lowercase();

        let unsigned = col_type_raw.contains("unsigned");
        let col_type = match col_type_raw {
            ctr if ctr.contains("tinyint") => {
                if unsigned {
                    ColType::UnsignedTiny
                } else {
                    ColType::Tiny
                }
            }

            ctr if ctr.contains("smallint") => {
                if unsigned {
                    ColType::UnsignedShort
                } else {
                    ColType::Short
                }
            }

            ctr if ctr.contains("bigint") => {
                if unsigned {
                    ColType::UnsignedLongLong
                } else {
                    ColType::LongLong
                }
            }

            ctr if ctr.contains("mediumint") || ctr.contains("int") => {
                if unsigned {
                    ColType::UnsignedLong
                } else {
                    ColType::Long
                }
            }

            ctr if ctr.contains("tinytext")
                || ctr.contains("mediumtext")
                || ctr.contains("longtext")
                || ctr.contains("text")
                || ctr.contains("varchar")
                || ctr.contains("char") =>
            {
                ColType::String
            }

            ctr if ctr.contains("tinyblob")
                || ctr.contains("mediumblob")
                || ctr.contains("longblob")
                || ctr.contains("varbinary")
                || ctr.contains("binary")
                || ctr.contains("blob") =>
            {
                ColType::Blob
            }

            ctr if ctr.contains("float") => ColType::Float,
            ctr if ctr.contains("double") => ColType::Double,
            ctr if ctr.contains("decimal") => ColType::Decimal,
            ctr if ctr.contains("datetime") => ColType::DateTime,
            ctr if ctr.contains("timestamp") => ColType::Timestamp,
            ctr if ctr.contains("date") => ColType::Date,
            ctr if ctr.contains("time") => ColType::Time,
            ctr if ctr.contains("year") => ColType::Year,
            ctr if ctr.contains("enum") => ColType::Enum,
            ctr if ctr.contains("set") => ColType::Set,
            ctr if ctr.contains("bit") => ColType::Bit,
            ctr if ctr.contains("json") => ColType::Json,

            // TODO
            // "geometry": "geometrycollection": "linestring": "multilinestring":
            // "multipoint": "multipolygon": "polygon": "point"
            _ => ColType::Unkown,
        };

        Ok(ColMeta {
            name: col_name,
            typee: col_type,
        })
    }

    async fn parse_keys(&self, db: &str, tb: &str) -> Result<HashMap<String, Vec<String>>, Error> {
        let mut key_map: HashMap<String, Vec<String>> = HashMap::new();
        let sql = format!("SHOW INDEXES FROM {}.{}", db, tb);
        let mut rows = sqlx::query(&sql).fetch(self.conn_pool);
        while let Some(row) = rows.try_next().await? {
            let non_unique: i8 = row.try_get("Non_unique")?;
            if non_unique == 1 {
                continue;
            }

            let mut key_name: String = row.try_get("Key_name")?;
            let mut col_name: String = row.try_get("Column_name")?;
            key_name = key_name.to_lowercase();
            col_name = col_name.to_lowercase();
            if let Some(key_cols) = key_map.get_mut(&key_name) {
                key_cols.push(col_name);
            } else {
                key_map.insert(key_name, vec![col_name]);
            }
        }
        Ok(key_map)
    }

    fn get_order_col(key_map: &HashMap<String, Vec<String>>) -> Result<Option<String>, Error> {
        // use primary key first
        if let Some(cols) = key_map.get("primary") {
            if cols.len() == 1 {
                return Ok(Some(cols.get(0).unwrap().clone()));
            }
        }

        for (_, cols) in key_map.iter() {
            if cols.len() == 1 {
                return Ok(Some(cols.get(0).unwrap().clone()));
            }
        }
        Ok(Option::None)
    }

    fn get_where_cols(
        key_map: &HashMap<String, Vec<String>>,
        col_names: &Vec<String>,
    ) -> Result<Vec<String>, Error> {
        if let Some(cols) = key_map.get("primary") {
            return Ok(cols.clone());
        }

        if let Some(cols) = key_map.values().next() {
            return Ok(cols.clone());
        }

        Ok(col_names.clone())
    }
}
