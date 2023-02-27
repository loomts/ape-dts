use bytes::Bytes;
use sqlx::{postgres::PgRow, Row};

use crate::{
    error::Error,
    meta::{col_value::ColValue, pg::pg_col_type::PgColType},
};

pub struct PgColValueConvertor {}

impl PgColValueConvertor {
    pub fn from_wal(col_type: &PgColType, value: &Bytes) -> Result<ColValue, Error> {
        let value_str = std::str::from_utf8(value).unwrap();

        // TODO
        if col_type.parent_oid != 0 {}
        if col_type.is_array() {}
        if col_type.is_enum() {}

        // include all types from https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
        // plus aliases from the shorter names produced by older wal2json
        let col_value = match col_type.name.as_str() {
            "boolean" | "bool" => ColValue::Bool("t" == value_str.to_lowercase()),

            "hstore" | "character" | "char" | "character varying" | "varchar" | "bpchar"
            | "text" | "geometry" | "geography" | "citext" | "bit" | "bit varying" | "varbit"
            | "json" | "jsonb" | "xml" | "uuid" | "tsrange" | "tstzrange" | "daterange"
            | "inet" | "cidr" | "macaddr" | "macaddr8" | "int4range" | "numrange" | "int8range" => {
                ColValue::String(value_str.to_string())
            }

            "integer" | "int" | "int4" | "smallint" | "int2" | "smallserial" | "serial"
            | "serial2" | "serial4" => {
                let res: i32 = value_str.parse().unwrap();
                ColValue::Long(res)
            }

            "bigint" | "bigserial" | "int8" | "oid" => {
                let res: i64 = value_str.parse().unwrap();
                ColValue::LongLong(res)
            }

            "real" | "float4" => {
                let res: f32 = value_str.parse().unwrap();
                ColValue::Float(res)
            }

            "double precision" | "float8" => {
                let res: f64 = value_str.parse().unwrap();
                ColValue::Double(res)
            }

            "numeric" | "decimal" => ColValue::Decimal(value_str.to_string()),

            "date" => ColValue::String(value_str.to_string()),

            "timestamp with time zone" | "timestamptz" => {
                ColValue::Timestamp(value_str.to_string())
            }

            "timestamp" | "timestamp without time zone" => {
                ColValue::DateTime(value_str.to_string())
            }

            "time" => ColValue::Time(value_str.to_string()),

            "time without time zone" => ColValue::Time(value_str.to_string()),

            "time with time zone" | "timetz" => ColValue::Time(value_str.to_string()),

            "bytea" => ColValue::Blob(value_str.as_bytes().to_vec()),

            // these are all PG-specific types
            "box" | "circle" | "interval" | "line" | "lseg" | "money" | "path" | "point"
            | "polygon" => ColValue::String(value_str.to_string()),

            "pg_lsn" | "tsquery" | "tsvector" | "txid_snapshot" => {
                ColValue::String(value_str.to_string())
            }

            _ => ColValue::String(value_str.to_string()),
        };

        Ok(col_value)
    }

    pub fn from_query(
        row: &PgRow,
        col_name: &str,
        col_type: &PgColType,
    ) -> Result<ColValue, Error> {
        let value: Option<Vec<u8>> = row.get_unchecked(col_name);
        if let None = value {
            return Ok(ColValue::None);
        }

        match col_type.name.as_str() {
            "boolean" | "bool" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::Bool("t" == value.to_lowercase()));
            }

            "hstore" | "character" | "char" | "character varying" | "varchar" | "bpchar"
            | "text" | "geometry" | "geography" | "citext" | "bit" | "bit varying" | "varbit"
            | "json" | "jsonb" | "xml" | "uuid" | "tsrange" | "tstzrange" | "daterange"
            | "inet" | "cidr" | "macaddr" | "macaddr8" | "int4range" | "numrange" | "int8range" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::String(value));
            }

            "integer" | "int" | "int4" | "smallint" | "int2" | "smallserial" | "serial"
            | "serial2" | "serial4" => {
                let value: i32 = row.try_get(col_name)?;
                return Ok(ColValue::Long(value));
            }

            "bigint" | "bigserial" | "int8" | "oid" => {
                let value: i64 = row.try_get(col_name)?;
                return Ok(ColValue::LongLong(value));
            }

            "real" | "float4" => {
                let value: f32 = row.try_get(col_name)?;
                return Ok(ColValue::Float(value));
            }

            "double precision" | "float8" => {
                let value: f64 = row.try_get(col_name)?;
                return Ok(ColValue::Double(value));
            }

            "numeric" | "decimal" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::Decimal(value));
            }

            "date" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::String(value));
            }

            "timestamp with time zone" | "timestamptz" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::Timestamp(value));
            }

            "timestamp" | "timestamp without time zone" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::DateTime(value));
            }

            "time" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::Time(value));
            }

            "time without time zone" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::String(value));
            }

            "time with time zone" | "timetz" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::String(value));
            }

            "bytea" => {
                let value: Vec<u8> = row.try_get(col_name)?;
                return Ok(ColValue::Blob(value));
            }

            // these are all PG-specific types
            "box" | "circle" | "interval" | "line" | "lseg" | "money" | "path" | "point"
            | "polygon" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::String(value));
            }

            "pg_lsn" | "tsquery" | "tsvector" | "txid_snapshot" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::String(value));
            }

            _ => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::String(value));
            }
        }
    }
}
