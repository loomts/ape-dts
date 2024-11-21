use std::collections::HashMap;

use anyhow::{bail, Context};
use duckdb::Connection;
use nom::ParseTo;

use crate::{
    error::Error,
    meta::{ddl_meta::ddl_data::DdlData, rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta},
};

use super::{duckdb_col_type::DuckdbColType, duckdb_tb_meta::DuckdbTbMeta};

pub struct DuckdbMetaManager {
    pub conn: Option<Connection>,
    pub cache: HashMap<String, DuckdbTbMeta>,
}

// duckdb::Connection is NOT Sync
unsafe impl Sync for DuckdbMetaManager {}

impl Clone for DuckdbMetaManager {
    fn clone(&self) -> Self {
        DuckdbMetaManager {
            cache: self.cache.clone(),
            conn: Some(self.conn.as_ref().unwrap().try_clone().unwrap()),
        }
    }
}

impl DuckdbMetaManager {
    pub fn new(conn: Connection) -> anyhow::Result<Self> {
        Ok(Self {
            conn: Some(conn),
            cache: HashMap::new(),
        })
    }

    pub fn close(&mut self) -> anyhow::Result<()> {
        // wrapping self.conn into Option<Connection> is a work around for:
        // DuckdbMetaManager::close has param: &self
        // duckdb::Connection::close has param: self
        if let Some(conn) = self.conn.take() {
            if let Err((_, err)) = conn.close() {
                bail!(Error::DuckdbError(err))
            }
        }
        Ok(())
    }

    pub fn invalidate_cache_by_ddl_data(&mut self, ddl_data: &DdlData) {
        let (schema, tb) = ddl_data.get_schema_tb();
        self.invalidate_cache(&schema, &tb);
    }

    pub fn invalidate_cache(&mut self, schema: &str, tb: &str) {
        if !schema.is_empty() && !tb.is_empty() {
            let full_name = format!(r#""{}"."{}""#, schema, tb).to_lowercase();
            self.cache.remove(&full_name);
        } else {
            self.cache.clear();
        }
    }

    pub fn get_tb_meta<'a>(
        &'a mut self,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<&'a DuckdbTbMeta> {
        let full_name = format!(r#""{}"."{}""#, schema, tb).to_lowercase();
        if !self.cache.contains_key(&full_name) {
            let (cols, col_type_map) = self.parse_cols(schema, tb)?;
            let key_map = self.parse_keys(schema, tb)?;
            let (order_col, partition_col, id_cols) =
                RdbMetaManager::parse_rdb_cols(&key_map, &cols)?;
            let basic = RdbTbMeta {
                schema: schema.to_string(),
                tb: tb.to_string(),
                cols,
                key_map,
                id_cols,
                order_col,
                partition_col,
                ..Default::default()
            };
            let tb_meta = DuckdbTbMeta {
                basic,
                col_type_map,
            };
            self.cache.insert(full_name.clone(), tb_meta);
        }
        Ok(self.cache.get(&full_name).unwrap())
    }

    fn parse_keys(&self, schema: &str, tb: &str) -> anyhow::Result<HashMap<String, Vec<String>>> {
        let mut key_map: HashMap<String, Vec<String>> = HashMap::new();
        let sql = format!(
            "SELECT
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema='{}' AND tc.table_name = '{}'",
            schema, tb
        );

        let mut stmt = self.conn.as_ref().unwrap().prepare(&sql)?;
        let mut rows = stmt.query([])?;
        while let Ok(Some(row)) = rows.next() {
            let mut key_name: String = row.get(0)?;
            let key_type: String = row.get(1)?;
            let mut col: String = row.get(2)?;
            key_name = key_name.to_lowercase();
            col = col.to_lowercase();

            if key_type.to_lowercase() == "primary key" {
                key_name = "primary".to_string();
            }

            if let Some(key_cols) = key_map.get_mut(&key_name) {
                key_cols.push(col);
            } else {
                key_map.insert(key_name, vec![col]);
            }
        }
        Ok(key_map)
    }

    fn parse_cols(
        &self,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<(Vec<String>, HashMap<String, DuckdbColType>)> {
        let mut cols = vec![];
        let mut col_type_map = HashMap::new();

        let sql: String = format!(r#"DESC "{}"."{}""#, schema, tb);
        let mut stmt = self.conn.as_ref().unwrap().prepare(&sql)?;
        let mut rows = stmt.query([])?;
        while let Ok(Some(row)) = rows.next() {
            let col: String = row.get(0)?;
            let col_type_str: String = row.get(1)?;
            cols.push(col.clone());
            col_type_map.insert(col.clone(), Self::get_col_type(&col, &col_type_str)?);
        }

        if cols.is_empty() {
            bail! {Error::MetadataError(format!(
                r#"failed to get duckdb table metadata for: "{}"."{}""#,
                schema, tb
            )) }
        }
        Ok((cols, col_type_map))
    }

    fn get_col_type(col: &str, col_type_str: &str) -> anyhow::Result<DuckdbColType> {
        let col_type_str = col_type_str.to_uppercase();
        let col_type = match col_type_str.as_str() {
            "TINYINT" => DuckdbColType::TinyInt,
            "SMALLINT" => DuckdbColType::SmallInt,
            "INTEGER" => DuckdbColType::Integer,
            "BIGINT" => DuckdbColType::BigInt,
            "UTINYINT" => DuckdbColType::UTinyInt,
            "USMALLINT" => DuckdbColType::USmallInt,
            "UINTEGER" => DuckdbColType::UInteger,
            "UBIGINT" => DuckdbColType::UBigInt,
            "FLOAT" => DuckdbColType::Float,
            "DOUBLE" => DuckdbColType::Double,
            "TIMESTAMP" => DuckdbColType::Timestamp,
            "DATE" => DuckdbColType::Date,
            "INTERVAL" => DuckdbColType::Interval,
            "DATETIME" => DuckdbColType::DateTime,
            "VARCHAR" => DuckdbColType::Varchar,
            "BLOB" => DuckdbColType::Blob,
            "JSON" => DuckdbColType::Json,
            _ => {
                if col_type_str.starts_with("ENUM") {
                    DuckdbColType::Enum
                } else if col_type_str.starts_with("DECIMAL") {
                    let mut tokens = vec![];
                    for str in col_type_str
                        .trim_start_matches("DECIMAL(")
                        .trim_end_matches(')')
                        .split(',')
                    {
                        let i: u32 = str.parse_to().with_context(|| {
                            format!("failed to parse duckdb decimal column metadata, col: {}, col_type: {}", col, col_type_str)
                        })?;
                        tokens.push(i);
                    }

                    if tokens.len() == 2 {
                        DuckdbColType::Decimal {
                            precision: tokens[0],
                            scale: tokens[1],
                        }
                    } else {
                        // should never happen
                        DuckdbColType::Decimal {
                            precision: 0,
                            scale: 0,
                        }
                    }
                } else {
                    DuckdbColType::Unknown
                }
            }
        };
        Ok(col_type)
    }
}
