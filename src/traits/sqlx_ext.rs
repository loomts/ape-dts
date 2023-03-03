use std::str::FromStr;

use bytes::Bytes;
use sqlx::{
    mysql::MySqlArguments,
    postgres::PgArguments,
    query::Query,
    types::{ipnetwork::IpNetwork, mac_address::MacAddress, Decimal},
    MySql, Postgres,
};

use crate::meta::col_value::ColValue;

pub trait SqlxMysql<'q> {
    fn bind_col_value<'b: 'q>(self, col_value: Option<&'b ColValue>) -> Self;
}

pub trait SqlxPg<'q> {
    fn bind_col_value<'b: 'q>(self, col_value: Option<&'b ColValue>) -> Self;
}

impl<'q> SqlxMysql<'q> for Query<'q, MySql, MySqlArguments> {
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

// impl<'q> SqlxPg<'q> for Query<'q, Postgres, PgArguments> {
//     fn bind_col_value<'b: 'q>(self, col_type: &PgColType, col_value: Option<&'b ColValue>) -> Self {
//         if let Some(value) = col_value {
//             match value {
//                 ColValue::Bool(v) => self.bind(v),
//                 ColValue::Tiny(v) => self.bind(v),
//                 ColValue::Short(v) => self.bind(v),
//                 ColValue::Long(v) => self.bind(v),
//                 ColValue::LongLong(v) => self.bind(v),
//                 ColValue::Float(v) => self.bind(v),
//                 ColValue::Double(v) => self.bind(v),
//                 ColValue::Decimal(v) => {
//                     if v.contains("NaN") {
//                         return self.bind(f64::NAN);
//                     } else if v.is_empty() {
//                         let none: Option<Decimal> = Option::None;
//                         return self.bind(none);
//                     } else {
//                         let decimal = Decimal::from_str(v.as_str()).unwrap();
//                         return self.bind(decimal);
//                     }
//                 }
//                 ColValue::Time(v) => self.bind(v),
//                 ColValue::Date(v) => self.bind(v),
//                 ColValue::DateTime(v) => self.bind(v),
//                 ColValue::Timestamp(v) => self.bind(v),
//                 ColValue::String(v) => match col_type.name.as_str() {
//                     "inet" | "cidr" => {
//                         let value = IpNetwork::from_str(v).unwrap();
//                         self.bind(value)
//                     }

//                     "macaddr" | "macaddr8" => {
//                         let value = MacAddress::from_str(v).unwrap();
//                         self.bind(value)
//                     }

//                     _ => self.bind(v),
//                 },
//                 ColValue::Blob(v) => self.bind(v),
//                 ColValue::Set2(v) => self.bind(v),
//                 ColValue::Enum2(v) => self.bind(v),
//                 ColValue::Json(v) => self.bind(v),
//                 ColValue::PgMoney(v) => self.bind(v),
//                 ColValue::BitVec(v) => self.bind(v),
//                 ColValue::PgTimeTz(v) => self.bind(v),
//                 ColValue::Interval(v) => self.bind(v),
//                 ColValue::NaiveDateTime(v) => self.bind(v),
//                 ColValue::DateTime2(v) => self.bind(v),
//                 ColValue::NaiveDate(v) => self.bind(v),
//                 ColValue::NaiveTime(v) => self.bind(v),
//                 ColValue::None => {
//                     if col_type.name == "decimal" || col_type.name == "numeric" {
//                         let none: Option<Decimal> = Option::None;
//                         return self.bind(none);
//                     } else {
//                         println!("none --- 1 ");
//                         let none: Option<String> = Option::None;
//                         self.bind(none)
//                     }
//                 }
//                 _ => {
//                     println!("none --- 2 ");
//                     let none: Option<String> = Option::None;
//                     self.bind(none)
//                 }
//             }
//         } else {
//             println!("none --- 3 ");
//             let none: Option<String> = Option::None;
//             self.bind(none)
//         }
//     }

//     fn bind_col_value2<'b: 'q>(self, col_value: Option<&'b sqlx::postgres::PgValue>) -> Self {
//         // let v = col_value.unwrap();
//         // self.bind(v);
//         self
//     }
// }

impl<'q> SqlxPg<'q> for Query<'q, Postgres, PgArguments> {
    fn bind_col_value<'b: 'q>(self, col_value: Option<&'b ColValue>) -> Self {
        if let Some(value) = col_value {
            match value {
                ColValue::Bool(v) => self.bind(v),
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
                // ColValue::PgMoney(v) => self.bind(v),
                // ColValue::BitVec(v) => self.bind(v),
                // ColValue::PgTimeTz(v) => self.bind(v),
                // ColValue::Interval(v) => self.bind(v),
                // ColValue::NaiveDateTime(v) => self.bind(v),
                // ColValue::DateTime2(v) => self.bind(v),
                // ColValue::NaiveDate(v) => self.bind(v),
                // ColValue::NaiveTime(v) => self.bind(v),
                _ => {
                    println!("none --- 2 ");
                    let none: Option<String> = Option::None;
                    self.bind(none)
                }
            }
        } else {
            println!("none --- 3 ");
            let none: Option<String> = Option::None;
            self.bind(none)
        }
    }
}
