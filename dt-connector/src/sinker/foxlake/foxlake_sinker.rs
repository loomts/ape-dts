use anyhow::Context;
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Timelike, Utc};
use dt_common::{
    config::{config_enums::ExtractType, s3_config::S3Config}, log_info, meta::{
        col_value::ColValue, ddl_data::DdlData, mysql::{mysql_col_type::MysqlColType, mysql_tb_meta::MysqlTbMeta}, rdb_meta_manager::RdbMetaManager, row_data::RowData, row_type::RowType
    }, monitor::monitor::Monitor, utils::time_util::TimeUtil
};
use orc_format::{
    schema::{Field, Schema},
    writer::{data::GenericData, Config, Writer},
};
use rusoto_core::ByteStream;
use rusoto_s3::{PutObjectRequest, S3Client, S3};
use rust_decimal::Decimal;
use sqlx::{MySql, Pool};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

use crate::{call_batch_fn, Sinker};

pub struct FoxlakeSinker {
    pub batch_size: usize,
    pub meta_manager: RdbMetaManager,
    pub monitor: Arc<Mutex<Monitor>>,
    pub s3_client: S3Client,
    pub s3_config: S3Config,
    pub conn_pool: Pool<MySql>,
    pub extract_type: ExtractType,
}

const CDC_ACTION: &str = "cdc_action";
const CDC_LOG_SEQUENCE: &str = "cdc_log_sequence";

#[async_trait]
impl Sinker for FoxlakeSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        call_batch_fn!(self, data, Self::batch_sink);
        Ok(())
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        for ddl_data in data.iter() {
            log_info!("sink ddl: {}", ddl_data.query);
            let query = sqlx::query(&ddl_data.query);
            query.execute(&self.conn_pool).await?;
        }
        Ok(())
    }
}

impl FoxlakeSinker {
    async fn batch_sink(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let tb_meta = self
            .meta_manager
            .mysql_meta_manager
            .as_mut()
            .unwrap()
            .get_tb_meta_by_row_data(&data[0])
            .await?
            .to_owned();

        let (orc_data, insert_only) = self
            .generate_orc_data(data, start_index, batch_size, &tb_meta)
            .await?;

        let s3_file = self.get_log_dml_file();
        self.put_to_s3(&s3_file, orc_data).await?;

        self.merge_to_foxlake(&tb_meta, &s3_file, insert_only).await
    }

    async fn merge_to_foxlake(
        &self,
        tb_meta: &MysqlTbMeta,
        s3_file: &str,
        insert_only: bool,
    ) -> anyhow::Result<()> {
        let s3 = &self.s3_config;
        let url = format!("{}/{}", s3.root_url, s3_file);

        let insert_only = if insert_only { "TRUE" } else { "FALSE" };
        let sql = format!(
            r#"MERGE INTO TABLE `{}`.`{}` USING URL = '{}' CREDENTIALS = (ACCESS_KEY_ID='{}' SECRET_ACCESS_KEY='{}') FILE_FORMAT = (TYPE='DML_CHANGE_LOG') INSERT_ONLY = {};"#,
            tb_meta.basic.schema, tb_meta.basic.tb, url, s3.access_key, s3.secret_key, insert_only
        );

        let query = sqlx::query(&sql);
        query
            .execute(&self.conn_pool)
            .await
            .with_context(|| format!("merge to foxlake failed: {}", sql))?;
        Ok(())
    }

    #[inline(always)]
    fn get_col_value<'a>(row_data: &'a RowData, col: &str) -> Option<&'a ColValue> {
        if row_data.row_type == RowType::Delete {
            row_data.before.as_ref().unwrap().get(col)
        } else {
            row_data.after.as_ref().unwrap().get(col)
        }
    }
}

// ORC functions
impl FoxlakeSinker {
    async fn generate_orc_data(
        &self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
        tb_meta: &MysqlTbMeta,
    ) -> anyhow::Result<(Vec<u8>, bool)> {
        let mut insert_only = true;
        let (tb_schema, fields) = self.get_tb_orc_schema(tb_meta)?;
        let col_count = tb_meta.basic.cols.len();

        let mut buffer = Vec::new();
        let mut writer = Writer::new(&mut buffer, &tb_schema, Config::new()).unwrap();
        let root = writer.data().unwrap_struct();

        // ignore cdc_action and cdc_log_sequence
        for (i, field) in fields.into_iter().take(col_count).enumerate() {
            let col = &field.0;
            let col_schema = field.1;
            match col_schema {
                Schema::Long => {
                    let field_data = root.child(i).unwrap_long();
                    for row_data in data.iter().skip(start_index).take(batch_size) {
                        match Self::get_col_value(row_data, col) {
                            Some(ColValue::Tiny(v)) => field_data.write(*v as i64),
                            Some(ColValue::UnsignedTiny(v)) => field_data.write(*v as i64),
                            Some(ColValue::Short(v)) => field_data.write(*v as i64),
                            Some(ColValue::UnsignedShort(v)) => field_data.write(*v as i64),
                            Some(ColValue::Long(v)) => field_data.write(*v as i64),
                            Some(ColValue::UnsignedLong(v)) => field_data.write(*v as i64),
                            Some(ColValue::LongLong(v)) => field_data.write(*v),
                            Some(ColValue::UnsignedLongLong(v)) => field_data.write(*v as i64),
                            Some(ColValue::Year(v)) => field_data.write(*v as i64),
                            Some(ColValue::Bit(v)) => field_data.write(*v as i64),
                            Some(ColValue::Set(v)) => field_data.write(*v as i64),
                            Some(ColValue::Enum(v)) => field_data.write(*v as i64),

                            Some(ColValue::Time(v)) => {
                                let timestamp = Self::time_to_long(v)?;
                                field_data.write(timestamp)
                            }

                            Some(ColValue::Date(v)) => {
                                let timestamp = Self::date_to_long(v)?;
                                field_data.write(timestamp)
                            }

                            Some(ColValue::DateTime(v)) | Some(ColValue::Timestamp(v)) => {
                                field_data.write(Self::timestamp_to_long(v)?)
                            }

                            _ => field_data.write_null(),
                        };
                    }
                }

                Schema::Decimal(..) => {
                    let field_data = root.child(i).unwrap_decimal();
                    for row_data in data.iter().skip(start_index).take(batch_size) {
                        match Self::get_col_value(row_data, col) {
                            Some(ColValue::Decimal(v)) => {
                                let decimal = Decimal::from_str(v)
                                    .with_context(|| format!("invalide decimal: {}", v))?;
                                field_data.write_i128(decimal.mantissa())
                            }
                            _ => field_data.write_null(),
                        };
                    }
                }

                Schema::Float => {
                    let field_data = root.child(i).unwrap_float();
                    for row_data in data.iter().skip(start_index).take(batch_size) {
                        match Self::get_col_value(row_data, col) {
                            Some(ColValue::Float(v)) => field_data.write(*v),
                            _ => field_data.write_null(),
                        };
                    }
                }

                Schema::Double => {
                    let field_data = root.child(i).unwrap_double();
                    for row_data in data.iter().skip(start_index).take(batch_size) {
                        match Self::get_col_value(row_data, col) {
                            Some(ColValue::Double(v)) => field_data.write(*v),
                            _ => field_data.write_null(),
                        };
                    }
                }

                Schema::String => {
                    let field_data = root.child(i).unwrap_string();
                    for row_data in data.iter().skip(start_index).take(batch_size) {
                        match Self::get_col_value(row_data, col) {
                            Some(ColValue::String(v))
                            | Some(ColValue::Set2(v))
                            | Some(ColValue::Enum2(v))
                            | Some(ColValue::Json2(v)) => field_data.write(v),
                            _ => field_data.write_null(),
                        };
                    }
                }

                Schema::Binary => {
                    let field_data = root.child(i).unwrap_binary();
                    for row_data in data.iter().skip(start_index).take(batch_size) {
                        match Self::get_col_value(row_data, col) {
                            Some(ColValue::Json(v))
                            | Some(ColValue::Blob(v))
                            | Some(ColValue::RawString(v)) => field_data.write(v),

                            Some(ColValue::Bit(v)) => {
                                let bit = Self::u64_to_bytes(*v);
                                field_data.write(&bit)
                            }

                            _ => field_data.write_null(),
                        };
                    }
                }
                // never happen
                _ => continue,
            }
        }

        let cdc_action = root.child(col_count).unwrap_long();
        for i in 0..batch_size {
            let action = match data[start_index + i].row_type {
                RowType::Insert => 0,
                RowType::Update => 1,
                RowType::Delete => 2,
            };
            cdc_action.write(action);
            if action != 0 {
                insert_only = false;
            }
        }

        let cdc_log_sequence = root.child(col_count + 1).unwrap_long();
        for _ in 0..batch_size {
            cdc_log_sequence.write(0);
        }

        for _ in 0..batch_size {
            root.write();
        }

        writer.write_batch(batch_size as u64)?;
        let orc_data = writer.finish()?.to_owned();

        Ok((orc_data, insert_only))
    }

    fn get_tb_orc_schema(&self, tb_meta: &MysqlTbMeta) -> anyhow::Result<(Schema, Vec<Field>)> {
        let mut fields = Vec::new();
        for col in tb_meta.basic.cols.iter() {
            let col_type = tb_meta.get_col_type(col)?;
            let schema = self.get_col_orc_schema(col_type);
            fields.push(Field(col.to_owned(), schema))
        }
        fields.push(Field(CDC_ACTION.to_owned(), Schema::Int));
        fields.push(Field(CDC_LOG_SEQUENCE.to_owned(), Schema::Long));
        Ok((Schema::Struct(fields.clone()), fields))
    }

    fn get_col_orc_schema(&self, col_type: &MysqlColType) -> Schema {
        match *col_type {
            MysqlColType::Tiny
            | MysqlColType::UnsignedTiny
            | MysqlColType::Short
            | MysqlColType::UnsignedShort
            | MysqlColType::Medium
            | MysqlColType::UnsignedMedium
            | MysqlColType::Long
            | MysqlColType::UnsignedLong
            | MysqlColType::LongLong
            | MysqlColType::UnsignedLongLong => Schema::Long,

            MysqlColType::Float => Schema::Float,
            MysqlColType::Double => Schema::Double,
            MysqlColType::Decimal { precision, scale } => Schema::Decimal(precision, scale),

            MysqlColType::Year => Schema::Long,
            MysqlColType::Time
            | MysqlColType::Date
            | MysqlColType::DateTime
            | MysqlColType::Timestamp { .. } => Schema::Long,

            MysqlColType::Binary { .. }
            | MysqlColType::Bit
            | MysqlColType::Blob
            | MysqlColType::VarBinary { .. }
            | MysqlColType::Unkown => Schema::Binary,

            MysqlColType::String { .. } => match self.extract_type {
                ExtractType::Cdc => Schema::Binary,
                _ => Schema::String,
            },

            MysqlColType::Enum { .. } | MysqlColType::Set { .. } | MysqlColType::Json => {
                Schema::String
            }
        }
    }

    #[inline(always)]
    fn timestamp_to_long(timestamp: &str) -> anyhow::Result<i64> {
        let datetime: DateTime<Utc> = TimeUtil::datetime_from_utc_str(timestamp)?;
        let ymd = ((datetime.year() as i64 * 13 + datetime.month() as i64) << 5)
            | (datetime.day() as i64);
        let hms = ((datetime.hour() as i64) << 12)
            | ((datetime.minute() as i64) << 6)
            | (datetime.second() as i64);
        let second_part = datetime.timestamp_micros() - datetime.timestamp() * 1_000_000;
        let l = (((ymd << 17) | hms) << 24) + second_part;
        Ok(l)
    }

    #[inline(always)]
    fn date_to_long(timestamp: &str) -> anyhow::Result<i64> {
        let date = TimeUtil::date_from_str(timestamp)?;
        let ymd = ((date.year() as i64 * 13 + date.month() as i64) << 5) | (date.day() as i64);
        let l = ymd << (24 + 17);
        Ok(l)
    }

    #[inline(always)]
    fn time_to_long(time: &str) -> anyhow::Result<i64> {
        let timestamp = format!("1970-01-01 {}", time);
        let datetime = TimeUtil::datetime_from_utc_str(&timestamp)?;
        Ok(datetime.timestamp_micros())
    }

    #[inline(always)]
    fn u64_to_bytes(value: u64) -> Vec<u8> {
        // Big Endian
        let mut bytes = [0; 8];
        let mut v = value;
        let mut i: i32 = 7;
        while i >= 0 {
            bytes[i as usize] = (v & 0xFF) as u8;
            v >>= 8;
            i -= 1;
        }
        bytes.to_vec()
    }
}

// s3 functions
impl FoxlakeSinker {
    fn get_log_dml_file(&self) -> String {
        // currently we do not get sequence from position
        let log_sequence = "0_0";
        let file_name = format!("log_dml_{}_{}.orc", log_sequence, Uuid::new_v4());
        format!("{}/{}", self.s3_config.root_dir, file_name)
    }

    async fn put_to_s3(&self, key: &str, data: Vec<u8>) -> anyhow::Result<()> {
        let byte_stream = ByteStream::from(data);
        let request = PutObjectRequest {
            bucket: self.s3_config.bucket.clone(),
            key: key.to_string(),
            body: Some(byte_stream),
            ..Default::default()
        };

        self.s3_client
            .put_object(request)
            .await
            .with_context(|| format!("failed to push objects to s3, key: {}", key))?;
        Ok(())
    }
}
