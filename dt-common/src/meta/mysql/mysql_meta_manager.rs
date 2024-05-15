use std::collections::HashMap;

use crate::{config::config_enums::DbType, error::Error};
use anyhow::bail;
use futures::TryStreamExt;

use sqlx::{mysql::MySqlRow, MySql, Pool, Row};

use crate::meta::{
    foreign_key::ForeignKey, rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta,
    row_data::RowData,
};

use super::{mysql_col_type::MysqlColType, mysql_tb_meta::MysqlTbMeta};

#[derive(Clone)]
pub struct MysqlMetaManager {
    pub conn_pool: Pool<MySql>,
    pub cache: HashMap<String, MysqlTbMeta>,
    pub version: String,
    pub db_type: DbType,
}

const COLUMN_NAME: &str = "COLUMN_NAME";
const COLUMN_TYPE: &str = "COLUMN_TYPE";
const DATA_TYPE: &str = "DATA_TYPE";
const CHARACTER_MAXIMUM_LENGTH: &str = "CHARACTER_MAXIMUM_LENGTH";
const CHARACTER_SET_NAME: &str = "CHARACTER_SET_NAME";

impl MysqlMetaManager {
    pub fn new(conn_pool: Pool<MySql>) -> Self {
        Self::new_mysql_compatible(conn_pool, DbType::Mysql)
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.conn_pool.close().await;
        Ok(())
    }

    pub fn new_mysql_compatible(conn_pool: Pool<MySql>, db_type: DbType) -> Self {
        Self {
            conn_pool,
            cache: HashMap::new(),
            version: String::new(),
            db_type,
        }
    }

    pub async fn init(mut self) -> anyhow::Result<Self> {
        self.init_version().await?;
        Ok(self)
    }

    pub fn invalidate_cache(&mut self, schema: &str, tb: &str) {
        if !schema.is_empty() && !tb.is_empty() {
            let full_name = format!("{}.{}", schema, tb);
            self.cache.remove(&full_name);
        } else {
            // clear all cache is always safe
            self.cache.clear();
        }
    }

    pub async fn get_tb_meta_by_row_data<'a>(
        &'a mut self,
        row_data: &RowData,
    ) -> anyhow::Result<&'a MysqlTbMeta> {
        self.get_tb_meta(&row_data.schema, &row_data.tb).await
    }

    pub async fn get_tb_meta<'a>(
        &'a mut self,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<&'a MysqlTbMeta> {
        let full_name = format!("{}.{}", schema, tb);
        if !self.cache.contains_key(&full_name) {
            let (cols, col_type_map) =
                Self::parse_cols(&self.conn_pool, &self.db_type, &self.version, schema, tb).await?;
            let key_map = Self::parse_keys(&self.conn_pool, schema, tb).await?;
            let (order_col, partition_col, id_cols) =
                RdbMetaManager::parse_rdb_cols(&key_map, &cols)?;
            let foreign_keys =
                Self::get_foreign_keys(&self.conn_pool, &self.db_type, schema, tb).await?;

            let basic = RdbTbMeta {
                schema: schema.to_string(),
                tb: tb.to_string(),
                cols,
                key_map,
                order_col,
                partition_col,
                id_cols,
                foreign_keys,
            };
            let tb_meta = MysqlTbMeta {
                basic,
                col_type_map,
            };
            self.cache.insert(full_name.clone(), tb_meta);
        }
        Ok(self.cache.get(&full_name).unwrap())
    }

    async fn parse_cols(
        conn_pool: &Pool<MySql>,
        db_type: &DbType,
        version: &str,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<(Vec<String>, HashMap<String, MysqlColType>)> {
        let mut cols = Vec::new();
        let mut col_type_map = HashMap::new();

        let sql = format!("DESC `{}`.`{}`", schema, tb);
        let mut rows = sqlx::query(&sql).disable_arguments().fetch(conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let col_name: String = row.try_get("Field")?;
            cols.push(col_name);
        }

        let sql = if db_type == &DbType::Mysql {
            format!("SELECT {}, {}, {}, {}, {} FROM information_schema.columns WHERE table_schema = ? AND table_name = ?", 
                COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_SET_NAME)
        } else {
            // starrocks
            format!("SELECT {}, {}, {}, {}, {} FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}'", 
                COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_SET_NAME, &schema, &tb)
        };

        let mut rows = if db_type == &DbType::Mysql {
            sqlx::query(&sql).bind(schema).bind(tb).fetch(conn_pool)
        } else {
            // for starrocks
            sqlx::query(&sql).disable_arguments().fetch(conn_pool)
        };

        while let Some(row) = rows.try_next().await.unwrap() {
            let col: String = row.try_get(COLUMN_NAME)?;
            let col_type = Self::get_col_type(db_type, version, &row).await?;
            col_type_map.insert(col, col_type);
        }

        if cols.is_empty() {
            bail! {Error::MetadataError(format!(
                "failed to get table metadata for: `{}`.`{}`",
                schema, tb
            )) }
        }
        Ok((cols, col_type_map))
    }

    async fn get_col_type(
        db_type: &DbType,
        version: &str,
        row: &MySqlRow,
    ) -> anyhow::Result<MysqlColType> {
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

            "mediumint" => {
                if unsigned {
                    MysqlColType::UnsignedMedium
                } else {
                    MysqlColType::Medium
                }
            }

            "int" => {
                if unsigned {
                    MysqlColType::UnsignedLong
                } else {
                    MysqlColType::Long
                }
            }

            "varbinary" => {
                let length = Self::get_col_length(db_type, version, row).await?;
                MysqlColType::VarBinary {
                    length: length as u16,
                }
            }

            "binary" => {
                let length = Self::get_col_length(db_type, version, row).await?;
                MysqlColType::Binary {
                    length: length as u8,
                }
            }

            "varchar" | "char" | "tinytext" | "mediumtext" | "longtext" | "text" => {
                let length = Self::get_col_length(db_type, version, row).await?;
                let mut charset = String::new();
                let unchecked: Option<Vec<u8>> = row.get_unchecked(CHARACTER_SET_NAME);
                if unchecked.is_some() {
                    charset = row.try_get(CHARACTER_SET_NAME)?;
                }
                MysqlColType::String { length, charset }
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

    async fn get_col_length(
        db_type: &DbType,
        version: &str,
        row: &MySqlRow,
    ) -> anyhow::Result<u64> {
        // with A expression, error will throw for mysql 8.0: ColumnDecode { index: "\"CHARACTER_MAXIMUM_LENGTH\"", source: "mismatched types; Rust type `u64` (as SQL type `BIGINT UNSIGNED`) is not compatible with SQL type `BIGINT`" }'
        // with B expression, error will throw for mysql 5.7: ColumnDecode { index: "\"CHARACTER_MAXIMUM_LENGTH\"", source: "mismatched types; Rust type `i64` (as SQL type `BIGINT`) is not compatible with SQL type `BIGINT UNSIGNED`" }'
        // no need to consider versions before 5.*
        if db_type == &DbType::Mysql && version.starts_with("5.") {
            let length: u64 = row.try_get(CHARACTER_MAXIMUM_LENGTH).unwrap();
            Ok(length)
        } else {
            let length: i64 = row.try_get(CHARACTER_MAXIMUM_LENGTH).unwrap();
            Ok(length as u64)
        }
    }

    async fn parse_keys(
        conn_pool: &Pool<MySql>,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<HashMap<String, Vec<String>>> {
        let mut key_map: HashMap<String, Vec<String>> = HashMap::new();
        let sql = format!("SHOW INDEXES FROM `{}`.`{}`", schema, tb);
        let mut rows = sqlx::query(&sql).disable_arguments().fetch(conn_pool);
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

    async fn get_foreign_keys(
        conn_pool: &Pool<MySql>,
        db_type: &DbType,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<Vec<ForeignKey>> {
        let mut foreign_keys = Vec::new();
        if db_type == &DbType::StarRocks {
            return Ok(foreign_keys);
        }

        let sql = format!(
            "SELECT
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_SCHEMA,
                kcu.REFERENCED_TABLE_NAME,
                kcu.REFERENCED_COLUMN_NAME
            FROM
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA
            WHERE
                kcu.CONSTRAINT_SCHEMA = '{}'
                AND kcu.TABLE_NAME = '{}'
                AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'",
            schema, tb,
        );

        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let col: String = row.try_get("COLUMN_NAME")?;
            let ref_schema: String = row.try_get("REFERENCED_TABLE_SCHEMA")?;
            let ref_tb: String = row.try_get("REFERENCED_TABLE_NAME")?;
            let ref_col: String = row.try_get("REFERENCED_COLUMN_NAME")?;
            foreign_keys.push(ForeignKey {
                col: col.to_lowercase(),
                ref_schema: ref_schema.to_lowercase(),
                ref_tb: ref_tb.to_lowercase(),
                ref_col: ref_col.to_lowercase(),
            });
        }
        Ok(foreign_keys)
    }

    async fn init_version(&mut self) -> anyhow::Result<()> {
        let sql = "SELECT VERSION()";
        let mut rows = sqlx::query(sql).disable_arguments().fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await.unwrap() {
            let version: String = row.try_get(0)?;
            self.version = version.trim().into();
            return Ok(());
        }
        bail! {Error::MetadataError("failed to init mysql version".into())}
    }
}
