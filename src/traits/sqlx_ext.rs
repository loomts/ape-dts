use sqlx::{mysql::MySqlArguments, postgres::PgArguments, query::Query, MySql, Postgres};

use crate::meta::col_value::ColValue;

pub trait SqlxExt<'q> {
    fn bind_col_value<'b: 'q>(self, col_value: Option<&'b ColValue>) -> Self;
}

impl<'q> SqlxExt<'q> for Query<'q, MySql, MySqlArguments> {
    fn bind_col_value<'b: 'q>(self, col_value: Option<&'b ColValue>) -> Self {
        if let Some(value) = col_value {
            match value {
                ColValue::Tiny(v) => self.bind(v),
                ColValue::UnsignedTiny(v) => self.bind(v),
                ColValue::Short(v) => self.bind(v),
                ColValue::UnsignedShort(v) => self.bind(v),
                ColValue::Long(v) => self.bind(v),
                ColValue::UnsignedLong(v) => self.bind(v),
                ColValue::LongLong(v) => self.bind(v),
                ColValue::UnsignedLongLong(v) => self.bind(v),
                ColValue::Float(v) => self.bind(v),
                ColValue::Double(v) => self.bind(v),
                ColValue::Decimal(v) => self.bind(v),
                ColValue::Time(v) => self.bind(v),
                ColValue::Date(v) => self.bind(v),
                ColValue::DateTime(v) => self.bind(v),
                ColValue::Timestamp(v) => self.bind(v),
                ColValue::Year(v) => self.bind(v),
                ColValue::String(v) => self.bind(v),
                ColValue::Blob(v) => self.bind(v),
                ColValue::Bit(v) => self.bind(v),
                ColValue::Set(v) => self.bind(v),
                ColValue::Set2(v) => self.bind(v),
                ColValue::Enum(v) => self.bind(v),
                ColValue::Enum2(v) => self.bind(v),
                ColValue::Json(v) => self.bind(v),
                _ => {
                    let none: Option<String> = Option::None;
                    self.bind(none)
                }
            }
        } else {
            let none: Option<String> = Option::None;
            self.bind(none)
        }
    }
}

impl<'q> SqlxExt<'q> for Query<'q, Postgres, PgArguments> {
    fn bind_col_value<'b: 'q>(self, col_value: Option<&'b ColValue>) -> Self {
        if let Some(value) = col_value {
            match value {
                ColValue::Tiny(v) => self.bind(v),
                ColValue::Short(v) => self.bind(v),
                ColValue::Long(v) => self.bind(v),
                ColValue::LongLong(v) => self.bind(v),
                ColValue::Float(v) => self.bind(v),
                ColValue::Double(v) => self.bind(v),
                ColValue::Decimal(v) => self.bind(v),
                ColValue::Time(v) => self.bind(v),
                ColValue::Date(v) => self.bind(v),
                ColValue::DateTime(v) => self.bind(v),
                ColValue::Timestamp(v) => self.bind(v),
                ColValue::String(v) => self.bind(v),
                ColValue::Blob(v) => self.bind(v),
                ColValue::Set2(v) => self.bind(v),
                ColValue::Enum2(v) => self.bind(v),
                ColValue::Json(v) => self.bind(v),
                _ => {
                    let none: Option<String> = Option::None;
                    self.bind(none)
                }
            }
        } else {
            let none: Option<String> = Option::None;
            self.bind(none)
        }
    }
}
