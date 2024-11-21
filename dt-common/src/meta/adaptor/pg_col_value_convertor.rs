use bytes::Bytes;
use sqlx::{postgres::PgRow, Row};

use crate::meta::{
    col_value::ColValue,
    pg::{pg_col_type::PgColType, pg_meta_manager::PgMetaManager},
};

pub struct PgColValueConvertor {}

impl PgColValueConvertor {
    pub fn get_extract_type(col_type: &PgColType) -> String {
        let extract_type = match col_type.alias.as_str() {
            "bytea" => "bytea",

            "oid" => "int8",

            "citext" | "hstore" | "char" | "varchar" | "bpchar" | "text" | "geometry"
            | "geography" | "json" | "jsonb" | "xml" | "uuid" | "tsrange" | "tstzrange"
            | "daterange" | "inet" | "cidr" | "macaddr" | "macaddr8" | "int4range" | "numrange"
            | "int8range" => "text",

            "bit" | "varbit" => "text",

            "numeric" | "decimal" => "text",

            "date" | "timestamp" | "time" => "text",

            "timestamptz" | "timetz" => "text",

            "box" | "circle" | "interval" | "line" | "lseg" | "money" | "path" | "point"
            | "polygon" => "text",

            "pg_lsn" | "tsquery" | "tsvector" | "txid_snapshot" => "text",

            // no need to cast
            _ => "",
        };

        if extract_type.is_empty() && (col_type.is_array() || col_type.is_user_defined()) {
            return "text".to_string();
        }
        extract_type.to_string()
    }

    pub fn from_str(
        col_type: &PgColType,
        value_str: &str,
        meta_manager: &mut PgMetaManager,
    ) -> anyhow::Result<ColValue> {
        if col_type.parent_oid != 0 {
            let parent_col_type = meta_manager.get_col_type_by_oid(col_type.parent_oid)?;
            return Self::from_str(&parent_col_type, value_str, meta_manager);
        }

        let value_str = value_str.to_string();
        if col_type.is_array() {
            return Ok(ColValue::String(value_str));
        }

        let col_value = match col_type.alias.as_str() {
            "bool" => ColValue::Bool("t" == value_str.to_lowercase()),

            "int4" | "serial4" => {
                let res: i32 = value_str.parse()?;
                ColValue::Long(res)
            }

            "int2" | "serial2" => {
                let value: i16 = value_str.parse()?;
                ColValue::Short(value)
            }

            "int8" | "serial8" | "oid" => {
                let value: i64 = value_str.parse()?;
                ColValue::LongLong(value)
            }

            "float4" => {
                let value: f32 = value_str.parse()?;
                ColValue::Float(value)
            }

            "float8" => {
                let value: f64 = value_str.parse()?;
                ColValue::Double(value)
            }

            "bytea" => ColValue::String(value_str),

            "decimal" => ColValue::Decimal(value_str),

            "timestamptz" => ColValue::Timestamp(value_str),

            "timestamp" => ColValue::DateTime(value_str),

            "time" => ColValue::Time(value_str),

            "timetz" => ColValue::String(value_str),

            "date" => ColValue::String(value_str),

            "hstore" | "character" | "char" | "character varying" | "varchar" | "bpchar"
            | "text" | "geometry" | "geography" | "citext" | "bit" | "bit varying" | "varbit"
            | "json" | "jsonb" | "xml" | "uuid" | "tsrange" | "tstzrange" | "daterange"
            | "inet" | "cidr" | "macaddr" | "macaddr8" | "int4range" | "numrange" | "int8range"
            | "box" | "circle" | "interval" | "line" | "lseg" | "money" | "path" | "point"
            | "polygon" | "pg_lsn" | "tsquery" | "tsvector" | "txid_snapshot" => {
                ColValue::String(value_str)
            }

            _ => ColValue::String(value_str),
        };
        Ok(col_value)
    }

    pub fn from_wal(
        col_type: &PgColType,
        value: &Bytes,
        meta_manager: &mut PgMetaManager,
    ) -> anyhow::Result<ColValue> {
        // include all types from https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
        // plus aliases from the shorter names produced by older wal2json
        // let value = value.unwrap();
        let value_str = std::str::from_utf8(value)?;
        Self::from_str(col_type, value_str, meta_manager)
    }

    pub fn from_query(row: &PgRow, col: &str, col_type: &PgColType) -> anyhow::Result<ColValue> {
        let value: Option<Vec<u8>> = row.get_unchecked(col);
        if value.is_none() {
            return Ok(ColValue::None);
        }

        if col_type.is_array() {
            let value: String = row.try_get(col)?;
            return Ok(ColValue::String(value));
        }

        let col_value = match col_type.alias.as_str() {
            "bool" => {
                let value: bool = row.try_get(col)?;
                ColValue::Bool(value)
            }

            "int4" | "serial4" => {
                let value: i32 = row.try_get(col)?;
                ColValue::Long(value)
            }

            "int2" | "serial2" => {
                let value: i16 = row.try_get(col)?;
                ColValue::Short(value)
            }

            "int8" | "serial8" | "oid" => {
                let value: i64 = row.try_get(col)?;
                ColValue::LongLong(value)
            }

            "float4" => {
                let value: f32 = row.try_get(col)?;
                ColValue::Float(value)
            }

            "float8" => {
                let value: f64 = row.try_get(col)?;
                ColValue::Double(value)
            }

            "bytea" => {
                let value: Vec<u8> = row.try_get(col)?;
                ColValue::Blob(value)
            }

            "decimal" => {
                let value: String = row.try_get(col)?;
                ColValue::Decimal(value)
            }

            "timestamptz" => {
                let value: String = row.try_get(col)?;
                ColValue::Timestamp(value)
            }

            "timestamp" => {
                let value: String = row.try_get(col)?;
                ColValue::DateTime(value)
            }

            "time" => {
                let value: String = row.try_get(col)?;
                ColValue::Time(value)
            }

            "timetz" => {
                let value: String = row.try_get(col)?;
                ColValue::String(value)
            }

            "date" => {
                let value: String = row.try_get(col)?;
                ColValue::String(value)
            }

            "hstore" | "character" | "char" | "character varying" | "varchar" | "bpchar"
            | "text" | "geometry" | "geography" | "citext" | "bit" | "bit varying" | "varbit"
            | "json" | "jsonb" | "xml" | "uuid" | "tsrange" | "tstzrange" | "daterange"
            | "inet" | "cidr" | "macaddr" | "macaddr8" | "int4range" | "numrange" | "int8range"
            | "box" | "circle" | "interval" | "line" | "lseg" | "money" | "path" | "point"
            | "polygon" | "pg_lsn" | "tsquery" | "tsvector" | "txid_snapshot" => {
                let value: String = row.try_get(col)?;
                ColValue::String(value)
            }

            _ => {
                let value: String = row.try_get(col)?;
                ColValue::String(value)
            }
        };
        Ok(col_value)
    }
}
