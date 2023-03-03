use std::str::FromStr;

use bytes::Bytes;
use postgres_protocol::types::Inet;
use postgres_types::Timestamp;
use sqlx::{
    postgres::{
        types::{PgInterval, PgMoney, PgTimeTz},
        PgRow, PgValue,
    },
    types::{chrono, ipnetwork::IpNetwork, time::Time, BitVec, Decimal, Json},
    Row,
};

use crate::{
    error::Error,
    meta::{
        col_value::ColValue,
        pg::{pg_col_type::PgColType, pg_col_value::PgColValue, pg_meta_manager::PgMetaManager},
    },
};

pub struct PgColValueConvertor {}

impl PgColValueConvertor {
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

        let col_value = match col_type.name.as_str() {
            "boolean" | "bool" => ColValue::Bool("t" == value_str.to_lowercase()),

            "citext" => ColValue::String(value_str),

            "hstore" | "character" | "char" | "character varying" | "varchar" | "bpchar"
            | "text" | "geometry" | "geography" | "bit" | "bit varying" | "varbit" | "json"
            | "jsonb" | "xml" | "uuid" | "tsrange" | "tstzrange" | "daterange" | "inet"
            | "cidr" | "macaddr" | "macaddr8" | "int4range" | "numrange" | "int8range" => {
                ColValue::String(value_str)
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

            "numeric" | "decimal" => ColValue::Decimal(value_str),

            "date" => ColValue::String(value_str),

            "timestamp with time zone" | "timestamptz" => ColValue::Timestamp(value_str),

            "timestamp" | "timestamp without time zone" => ColValue::DateTime(value_str),

            "time" => ColValue::Time(value_str),

            "time without time zone" => ColValue::Time(value_str),

            "time with time zone" | "timetz" => ColValue::Time(value_str),

            // these are all PG-specific types
            "box" | "circle" | "interval" | "line" | "lseg" | "money" | "path" | "point"
            | "polygon" => ColValue::String(value_str),

            "pg_lsn" | "tsquery" | "tsvector" | "txid_snapshot" => ColValue::String(value_str),

            // others, e.g. enum
            _ => ColValue::String(value_str),
        };

        Ok(col_value)
    }

    pub fn from_query(
        row: &PgRow,
        col_name: &str,
        col_type: &PgColType,
    ) -> Result<ColValue, Error> {
        // let value: Option<Vec<u8>> = row.get_unchecked(col_name);
        // if let None = value {
        //     return Ok(ColValue::None);
        // }

        println!("+++++++++++: {}", col_name);

        if col_type.is_array() {
            let value: String = row.try_get(col_name)?;
            return Ok(ColValue::String(value));
        }

        match col_type.name.as_str() {
            "boolean" | "bool" => {
                let value: bool = row.try_get(col_name)?;
                return Ok(ColValue::Bool(value));
            }

            "citext" => {
                // TODO https://github.com/launchbadge/sqlx/issues/295
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::String(value));
            }

            "inet" | "cidr" => {
                let value: sqlx::types::ipnetwork::IpNetwork = row.try_get(col_name)?;
                return Ok(ColValue::String(value.to_string()));
            }

            "macaddr" | "macaddr8" => {
                let value: sqlx::types::mac_address::MacAddress = row.try_get(col_name)?;
                return Ok(ColValue::String(value.to_string()));
            }

            // TODO, json
            // "json" | "jsonb" => {
            //     let value: Json = row.try_get(col_name).unwrap();
            //     return Ok(ColValue::String(value.to_string()));

            //     // let value: json = row.try_get(col_name).unwrap();
            //     // return Ok(ColValue::String(value.to_string()));
            // }

            // TODO, xml
            // "xml" => {}
            "uuid" => {
                let value: sqlx::types::uuid::Uuid = row.try_get(col_name).unwrap();
                return Ok(ColValue::String(value.to_string()));
            }

            // TODO, the difference between decimal and bigdecimal
            "numeric" | "decimal" => {
                // let value: sqlx::types::Decimal = row.try_get(col_name);

                // let value: Option<Decimal> = row.try_get(col_name)?;
                // let a = value.to_decimal(locale_frac_digits);
                // return Ok(ColValue::Decimal(value.to_string()));

                let value = Self::parse_decimal(row, col_name);
                if let Ok(v) = value {
                    return Ok(ColValue::Decimal(v.to_string()));
                }

                if let Err(e) = value {
                    match e {
                        Error::SqlxError { error } => {
                            if error.to_string().contains("NaN") {
                                return Ok(ColValue::Decimal("NaN".to_string()));
                            }
                        }
                        _ => {}
                    }
                    return Ok(ColValue::Decimal("".to_string()));
                }

                return Ok(ColValue::Decimal("".to_string()));
            }

            "money" => {
                let value: PgMoney = row.try_get(col_name)?;
                return Ok(ColValue::PgMoney(value));
            }

            "bit" | "bit varying" | "varbit" => {
                let value: BitVec = row.try_get(col_name)?;
                return Ok(ColValue::BitVec(value));
            }

            "hstore" | "character" | "char" | "character varying" | "varchar" | "bpchar"
            | "text" | "geometry" | "geography" | "tsrange" | "tstzrange" | "daterange"
            | "int4range" | "numrange" | "int8range" => {
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

            "bigint" | "bigserial" | "int8" => {
                let value: i64 = row.try_get(col_name)?;
                return Ok(ColValue::LongLong(value));
            }

            "oid" => {
                // let value: sqlx::postgres::types::Oid = row.try_get(col_name)?;
                // return Ok(ColValue::UnsignedLong(value.0));
                let value: sqlx::postgres::types::Oid = row.try_get(col_name)?;
                return Ok(ColValue::LongLong(value.0 as i64));
            }

            "real" | "float4" => {
                let value: f32 = row.try_get(col_name)?;
                return Ok(ColValue::Float(value));
            }

            "double precision" | "float8" => {
                let value: f64 = row.try_get(col_name)?;
                return Ok(ColValue::Double(value));
            }

            "date" => {
                println!("11111111111");
                // TODO, handle infinity
                // bug: https://github.com/launchbadge/sqlx/issues/2234
                let value: Option<chrono::NaiveDate> = row.try_get(col_name)?;
                return Ok(ColValue::NaiveDate(value));
            }

            // TODO, consider non-utc timestamp
            "timestamp with time zone" | "timestamptz" => {
                // TODO, handle `NaiveDateTime + Duration` overflowed'
                println!("2222222222");
                let value: Option<chrono::DateTime<chrono::Utc>> = row.try_get(col_name).unwrap();
                return Ok(ColValue::DateTime2(value));
            }

            "timestamp" | "timestamp without time zone" => {
                println!("333333333");
                // let value: Option<chrono::NaiveDateTime> = row.try_get(col_name).unwrap();
                // return Ok(ColValue::NaiveDateTime(value));

                let value: String = row.try_get(col_name).unwrap();
                return Ok(ColValue::String(value));
            }

            "time" => {
                println!("44444444444444");
                let value: Option<chrono::NaiveTime> = row.try_get(col_name).unwrap();
                return Ok(ColValue::NaiveTime(value));
            }

            "time without time zone" => {
                println!("5555555555555");
                let value: Option<chrono::NaiveTime> = row.try_get(col_name).unwrap();
                return Ok(ColValue::NaiveTime(value));
            }

            "time with time zone" | "timetz" => {
                println!("66666666666666");
                let value: Option<PgTimeTz> = row.try_get(col_name).unwrap();
                return Ok(ColValue::PgTimeTz(value));
            }

            "interval" => {
                println!("7777777777777777");
                let value: Option<PgInterval> = row.try_get(col_name)?;
                return Ok(ColValue::Interval(value));
            }

            "bytea" => {
                let value: Vec<u8> = row.try_get(col_name)?;
                return Ok(ColValue::Blob(value));
            }

            // these are all PG-specific types
            "box" | "circle" | "line" | "lseg" | "path" | "point" | "polygon" => {
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

    fn parse_decimal(row: &PgRow, col_name: &str) -> Result<Decimal, Error> {
        let value: sqlx::types::Decimal = row.try_get(col_name)?;
        Ok(value)
    }

    // pub fn from_query_2(
    //     row: &PgRow,
    //     col_name: &str,
    //     col_type: &PgColType,
    // ) -> Result<PgValue, Error> {
    //     let value: Option<Bytes> = row.try_get(col_name);

    //     let v = PgValue {
    //         value,
    //         format: PgValueFormat::Binary,
    //         type_info: PgTypeInfo,
    //     };
    //     Ok(v)
    // }

    // pub fn to_pg_col_value(col_value: &ColValue, col_type: &PgColType) -> Result<PgValue, Error> {
    //     match col_value {
    //         ColValue::UnsignedLong(v) => match col_type.name.as_str() {
    //             "oid" => {
    //                 return Ok(ColValue::UnsignedLong(value.0));
    //             }
    //         },

    //         _ =>
    //     }
    // }
}
