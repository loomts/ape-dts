use std::collections::HashMap;

use futures::TryStreamExt;

use sqlx::{mysql::MySqlRow, MySql, Pool, Row};

use crate::error::Error;

use super::{col_meta::ColMeta, col_type::ColType, tb_meta::TbMeta};

pub struct DbMetaManager<'a> {
    pub conn_pool: &'a Pool<MySql>,
    pub cache: HashMap<String, TbMeta>,
    pub version: String,
    // timezone diff with utc in seconds
    pub timezone_diff_utc_seconds: i64,
}

const COLUMN_NAME: &str = "COLUMN_NAME";
const COLUMN_TYPE: &str = "COLUMN_TYPE";
const DATA_TYPE: &str = "DATA_TYPE";
const CHARACTER_MAXIMUM_LENGTH: &str = "CHARACTER_MAXIMUM_LENGTH";
const CHARACTER_SET_NAME: &str = "CHARACTER_SET_NAME";

impl<'a> DbMetaManager<'a> {
    pub fn new(conn_pool: &'a Pool<MySql>) -> Self {
        Self {
            conn_pool,
            cache: HashMap::new(),
            version: "".to_string(),
            timezone_diff_utc_seconds: 0,
        }
    }

    pub async fn init(mut self) -> Result<DbMetaManager<'a>, Error> {
        self.init_version().await?;
        self.init_timezone_diff_utc_seconds().await?;
        Ok(self)
    }

    pub async fn get_tb_meta(&mut self, db: &str, tb: &str) -> Result<TbMeta, Error> {
        let full_name = format!("{}.{}", db, tb);
        if let Some(tb_meta) = self.cache.get(&full_name) {
            return Ok(tb_meta.clone());
        }

        let (cols, col_meta_map) = self.parse_cols(db, tb).await?;
        let key_map = self.parse_keys(db, tb).await?;
        let order_col = Self::get_order_col(&key_map)?;
        let where_cols = Self::get_where_cols(&key_map, &cols)?;
        let tb_meta = TbMeta {
            db: db.to_string(),
            tb: tb.to_string(),
            cols,
            col_meta_map,
            key_map,
            order_col,
            where_cols,
        };
        self.cache.insert(full_name.clone(), tb_meta.clone());
        Ok(tb_meta)
    }

    async fn parse_cols(
        &mut self,
        db: &str,
        tb: &str,
    ) -> Result<(Vec<String>, HashMap<String, ColMeta>), Error> {
        let mut cols = Vec::new();
        let mut col_meta_map = HashMap::new();

        let sql = format!("SELECT {}, {}, {}, {}, {} FROM information_schema.columns WHERE table_schema = ? AND table_name = ?", 
            COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_SET_NAME);
        let mut rows = sqlx::query(&sql).bind(db).bind(tb).fetch(self.conn_pool);
        while let Some(row) = rows.try_next().await? {
            let col_type = self.parse_col_meta(&row).await?;
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

    async fn parse_col_meta(&mut self, row: &MySqlRow) -> Result<ColMeta, Error> {
        let col_name: String = row.try_get(COLUMN_NAME)?;
        let col_type: String = row.try_get(COLUMN_TYPE)?;
        let data_type: String = row.try_get(DATA_TYPE)?;

        let unsigned = col_type.to_lowercase().contains("unsigned");
        let typee = match data_type.as_str() {
            "tinyint" => {
                if unsigned {
                    ColType::UnsignedTiny
                } else {
                    ColType::Tiny
                }
            }

            "smallint" => {
                if unsigned {
                    ColType::UnsignedShort
                } else {
                    ColType::Short
                }
            }

            "bigint" => {
                if unsigned {
                    ColType::UnsignedLongLong
                } else {
                    ColType::LongLong
                }
            }

            "mediumint" | "int" => {
                if unsigned {
                    ColType::UnsignedLong
                } else {
                    ColType::Long
                }
            }

            "varbinary" => {
                let length = self.get_col_length(&row).await?;
                ColType::VarBinary {
                    length: length as u16,
                }
            }

            "binary" => {
                let length = self.get_col_length(&row).await?;
                ColType::Binary {
                    length: length as u8,
                }
            }

            "varchar" | "char" => {
                let length = self.get_col_length(&row).await?;
                ColType::String {
                    length,
                    charset: row.try_get(CHARACTER_SET_NAME)?,
                }
            }

            "timestamp" => ColType::Timestamp {
                timezone_diff_utc_seconds: self.timezone_diff_utc_seconds,
            },

            "tinytext" | "mediumtext" | "longtext" | "text" => ColType::Blob,
            "tinyblob" | "mediumblob" | "longblob" | "blob" => ColType::Blob,
            "float" => ColType::Float,
            "double" => ColType::Double,
            "decimal" => ColType::Decimal,
            "datetime" => ColType::DateTime,
            "date" => ColType::Date,
            "time" => ColType::Time,
            "year" => ColType::Year,
            "enum" => ColType::Enum,
            "set" => ColType::Set,
            "bit" => ColType::Bit,
            "json" => ColType::Json,

            // TODO
            // "geometry": "geometrycollection": "linestring": "multilinestring":
            // "multipoint": "multipolygon": "polygon": "point"
            _ => ColType::Unkown,
        };

        Ok(ColMeta {
            name: col_name,
            typee,
        })
    }

    async fn get_col_length(&mut self, row: &MySqlRow) -> Result<u64, Error> {
        // with A expression, error will throw for mysql 8.0: ColumnDecode { index: "\"CHARACTER_MAXIMUM_LENGTH\"", source: "mismatched types; Rust type `u64` (as SQL type `BIGINT UNSIGNED`) is not compatible with SQL type `BIGINT`" }'
        // with B expression, error will throw for mysql 5.7: ColumnDecode { index: "\"CHARACTER_MAXIMUM_LENGTH\"", source: "mismatched types; Rust type `i64` (as SQL type `BIGINT`) is not compatible with SQL type `BIGINT UNSIGNED`" }'
        if self.version.contains("5.7") {
            let length: u64 = row.try_get(CHARACTER_MAXIMUM_LENGTH).unwrap();
            Ok(length)
        } else {
            let length: i64 = row.try_get(CHARACTER_MAXIMUM_LENGTH).unwrap();
            Ok(length as u64)
        }
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

    async fn init_version(&mut self) -> Result<(), Error> {
        let sql = "SELECT VERSION()";
        let mut rows = sqlx::query(&sql).fetch(self.conn_pool);
        if let Some(row) = rows.try_next().await? {
            self.version = row.try_get(0)?;
            return Ok(());
        }

        Err(Error::MetadataError {
            error: "failed to init mysql version".to_string(),
        })
    }

    async fn init_timezone_diff_utc_seconds(&mut self) -> Result<(), Error> {
        let sql = "SELECT TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP, NOW())";
        let mut rows = sqlx::query(&sql).fetch(self.conn_pool);
        if let Some(row) = rows.try_next().await? {
            // by default, sqlx will use UTC(+00:00) for connections which TIMESTAMPDIFF is 0
            self.timezone_diff_utc_seconds = row.try_get(0)?;
            return Ok(());
        }

        Err(Error::MetadataError {
            error: "failed to init timestamp diff with utc".to_string(),
        })
    }
}
