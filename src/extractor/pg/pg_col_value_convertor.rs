use crate::{
    error::Error,
    meta::{
        col_value::ColValue,
        pg::{pg_col_type::PgColType, pg_meta_manager::PgMetaManager},
    },
};
use bytes::Bytes;
use sqlx::{postgres::PgRow, Row};

pub struct PgColValueConvertor {}

impl PgColValueConvertor {
    pub fn get_extract_type(col_type: &PgColType) -> String {
        let extract_type = match col_type.short_name.as_str() {
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

        if extract_type.is_empty() {
            if col_type.is_array() || col_type.is_user_defined() {
                return "text".to_string();
            }
        }
        extract_type.to_string()
    }

    pub fn from_wal(
        col_type: &PgColType,
        value: &Bytes,
        meta_manager: &mut PgMetaManager,
    ) -> Result<ColValue, Error> {
        if col_type.parent_oid != 0 {
            let parent_col_type = meta_manager.get_col_type_by_oid(col_type.parent_oid)?;
            return PgColValueConvertor::from_wal(&parent_col_type, value, meta_manager);
        }

        // include all types from https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
        // plus aliases from the shorter names produced by older wal2json
        let value_str = std::str::from_utf8(value).unwrap().to_string();

        if col_type.is_array() {
            return Ok(ColValue::String(value_str));
        }

        let col_value = match col_type.short_name.as_str() {
            "boolean" | "bool" => ColValue::Bool("t" == value_str.to_lowercase()),

            "hstore" | "character" | "char" | "character varying" | "varchar" | "bpchar"
            | "text" | "geometry" | "geography" | "citext" | "bit" | "bit varying" | "varbit"
            | "json" | "jsonb" | "xml" | "uuid" | "tsrange" | "tstzrange" | "daterange"
            | "inet" | "cidr" | "macaddr" | "macaddr8" | "int4range" | "numrange" | "int8range" => {
                ColValue::String(value_str)
            }

            "integer" | "int" | "int4" | "serial" | "serial2" | "serial4" => {
                let res: i32 = value_str.parse().unwrap();
                ColValue::Long(res)
            }

            "int2" | "smallserial" | "smallint" => {
                let value: i16 = value_str.parse().unwrap();
                return Ok(ColValue::Short(value));
            }

            "bigint" | "bigserial" | "int8" | "oid" => {
                let value: i64 = value_str.parse().unwrap();
                return Ok(ColValue::LongLong(value));
            }

            "real" | "float4" => {
                let value: f32 = value_str.parse().unwrap();
                return Ok(ColValue::Float(value));
            }

            "float8" => {
                let value: f64 = value_str.parse().unwrap();
                return Ok(ColValue::Double(value));
            }

            "numeric" | "decimal" => {
                return Ok(ColValue::Decimal(value_str));
            }

            "date" => {
                return Ok(ColValue::String(value_str));
            }

            "timestamptz" => {
                return Ok(ColValue::Timestamp(value_str));
            }

            "timestamp" => {
                return Ok(ColValue::DateTime(value_str));
            }

            "time" => {
                return Ok(ColValue::Time(value_str));
            }

            "timetz" => {
                return Ok(ColValue::String(value_str));
            }

            "bytea" => {
                return Ok(ColValue::String(value_str));
            }

            "box" | "circle" | "interval" | "line" | "lseg" | "money" | "path" | "point"
            | "polygon" => {
                let value: String = value_str.parse().unwrap();
                return Ok(ColValue::String(value));
            }

            "pg_lsn" | "tsquery" | "tsvector" | "txid_snapshot" => {
                let value: String = value_str.parse().unwrap();
                return Ok(ColValue::String(value));
            }

            _ => {
                return Ok(ColValue::String(value_str));
            }
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

        if col_type.is_array() {
            let value: String = row.try_get(col_name)?;
            return Ok(ColValue::String(value));
        }

        match col_type.short_name.as_str() {
            "bool" => {
                let value: bool = row.try_get(col_name)?;
                return Ok(ColValue::Bool(value));
            }

            "hstore" | "character" | "char" | "character varying" | "varchar" | "bpchar"
            | "text" | "geometry" | "geography" | "citext" | "bit" | "bit varying" | "varbit"
            | "json" | "jsonb" | "xml" | "uuid" | "tsrange" | "tstzrange" | "daterange"
            | "inet" | "cidr" | "macaddr" | "macaddr8" | "int4range" | "numrange" | "int8range" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::String(value));
            }

            "integer" | "int" | "int4" | "serial" | "serial2" | "serial4" => {
                let value: i32 = row.try_get(col_name)?;
                return Ok(ColValue::Long(value));
            }

            "int2" | "smallserial" | "smallint" => {
                let value: i16 = row.try_get(col_name)?;
                return Ok(ColValue::Short(value));
            }

            "bigint" | "bigserial" | "int8" | "oid" => {
                let value: i64 = row.try_get(col_name)?;
                return Ok(ColValue::LongLong(value));
            }

            "real" | "float4" => {
                let value: f32 = row.try_get(col_name)?;
                return Ok(ColValue::Float(value));
            }

            "float8" => {
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

            "timestamptz" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::Timestamp(value));
            }

            "timestamp" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::DateTime(value));
            }

            "time" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::Time(value));
            }

            "timetz" => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::String(value));
            }

            "bytea" => {
                let value: Vec<u8> = row.try_get(col_name)?;
                return Ok(ColValue::Blob(value));
            }

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
