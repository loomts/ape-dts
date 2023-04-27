use std::collections::HashMap;

use futures::TryStreamExt;

use sqlx::{mysql::MySqlRow, MySql, Pool, Row};

use crate::{
    error::Error,
    meta::{rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta},
};

use super::{mysql_col_type::MysqlColType, mysql_tb_meta::MysqlTbMeta};

#[derive(Clone)]
pub struct MysqlMetaManager {
    pub conn_pool: Pool<MySql>,
    pub cache: HashMap<String, MysqlTbMeta>,
    pub version: String,
}

const COLUMN_NAME: &str = "COLUMN_NAME";
const COLUMN_TYPE: &str = "COLUMN_TYPE";
const DATA_TYPE: &str = "DATA_TYPE";
const CHARACTER_MAXIMUM_LENGTH: &str = "CHARACTER_MAXIMUM_LENGTH";
const CHARACTER_SET_NAME: &str = "CHARACTER_SET_NAME";

impl<'a> MysqlMetaManager {
    pub fn new(conn_pool: Pool<MySql>) -> Self {
        Self {
            conn_pool,
            cache: HashMap::new(),
            version: "".to_string(),
        }
    }

    pub async fn init(mut self) -> Result<Self, Error> {
        self.init_version().await?;
        Ok(self)
    }

    pub async fn get_tb_meta(&mut self, schema: &str, tb: &str) -> Result<MysqlTbMeta, Error> {
        let full_name = format!("{}.{}", schema, tb);
        if let Some(tb_meta) = self.cache.get(&full_name) {
            return Ok(tb_meta.clone());
        }

        let (cols, col_type_map) = self.parse_cols(schema, tb).await?;
        let key_map = self.parse_keys(schema, tb).await?;
        let (order_col, partition_col, id_cols) = RdbMetaManager::parse_rdb_cols(&key_map, &cols)?;

        let basic = RdbTbMeta {
            schema: schema.to_string(),
            tb: tb.to_string(),
            cols,
            key_map,
            order_col,
            partition_col,
            id_cols,
        };
        let tb_meta = MysqlTbMeta {
            basic,
            col_type_map,
        };

        self.cache.insert(full_name.clone(), tb_meta.clone());
        Ok(tb_meta)
    }

    async fn parse_cols(
        &mut self,
        schema: &str,
        tb: &str,
    ) -> Result<(Vec<String>, HashMap<String, MysqlColType>), Error> {
        let mut cols = Vec::new();
        let mut col_type_map = HashMap::new();

        let sql = format!("DESC {}.{}", schema, tb);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let col_name: String = row.try_get("Field")?;
            cols.push(col_name);
        }

        let sql = format!("SELECT {}, {}, {}, {}, {} FROM information_schema.columns WHERE table_schema = ? AND table_name = ?", 
            COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_SET_NAME);
        let mut rows = sqlx::query(&sql)
            .bind(schema)
            .bind(tb)
            .fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let col: String = row.try_get(COLUMN_NAME)?;
            let col_type = self.get_col_type(&row).await?;
            col_type_map.insert(col, col_type);
        }

        if cols.is_empty() {
            return Err(Error::MetadataError {
                error: format!("failed to get table metadata for: {}.{}", schema, tb),
            });
        }
        Ok((cols, col_type_map))
    }

    async fn get_col_type(&mut self, row: &MySqlRow) -> Result<MysqlColType, Error> {
        let column_type: String = row.try_get(COLUMN_TYPE)?;
        let data_type: String = row.try_get(DATA_TYPE)?;

        let unsigned = column_type.to_lowercase().contains("unsigned");
        let col_type = match data_type.as_str() {
            "tinyint" => {
                if unsigned {
                    MysqlColType::UnsignedTiny
                } else {
                    MysqlColType::Tiny
                }
            }

            "smallint" => {
                if unsigned {
                    MysqlColType::UnsignedShort
                } else {
                    MysqlColType::Short
                }
            }

            "bigint" => {
                if unsigned {
                    MysqlColType::UnsignedLongLong
                } else {
                    MysqlColType::LongLong
                }
            }

            "mediumint" | "int" => {
                if unsigned {
                    MysqlColType::UnsignedLong
                } else {
                    MysqlColType::Long
                }
            }

            "varbinary" => {
                let length = self.get_col_length(&row).await?;
                MysqlColType::VarBinary {
                    length: length as u16,
                }
            }

            "binary" => {
                let length = self.get_col_length(&row).await?;
                MysqlColType::Binary {
                    length: length as u8,
                }
            }

            "varchar" | "char" | "tinytext" | "mediumtext" | "longtext" | "text" => {
                let length = self.get_col_length(&row).await?;
                MysqlColType::String {
                    length,
                    charset: row.try_get(CHARACTER_SET_NAME)?,
                }
            }

            // as a client of mysql, sqlx's client timezone is UTC by default,
            // so no matter what timezone of src/dst server is,
            // src server will convert the timestamp field into UTC for sqx,
            // and then sqx will write it into dst server by UTC,
            // and then dst server will convert the received UTC timestamp into its own timezone.
            "timestamp" => MysqlColType::Timestamp { timezone_offset: 0 },

            "tinyblob" | "mediumblob" | "longblob" | "blob" => MysqlColType::Blob,
            "float" => MysqlColType::Float,
            "double" => MysqlColType::Double,
            "decimal" => MysqlColType::Decimal,
            "datetime" => MysqlColType::DateTime,
            "date" => MysqlColType::Date,
            "time" => MysqlColType::Time,
            "year" => MysqlColType::Year,
            "enum" => MysqlColType::Enum,
            "set" => MysqlColType::Set,
            "bit" => MysqlColType::Bit,
            "json" => MysqlColType::Json,

            // TODO
            // "geometry": "geometrycollection": "linestring": "multilinestring":
            // "multipoint": "multipolygon": "polygon": "point"
            _ => MysqlColType::Unkown,
        };

        Ok(col_type)
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

    async fn parse_keys(
        &self,
        schema: &str,
        tb: &str,
    ) -> Result<HashMap<String, Vec<String>>, Error> {
        let mut key_map: HashMap<String, Vec<String>> = HashMap::new();
        let sql = format!("SHOW INDEXES FROM {}.{}", schema, tb);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
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

    async fn init_version(&mut self) -> Result<(), Error> {
        let sql = "SELECT VERSION()";
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await.unwrap() {
            self.version = row.try_get(0)?;
            return Ok(());
        }

        Err(Error::MetadataError {
            error: "failed to init mysql version".to_string(),
        })
    }
}
