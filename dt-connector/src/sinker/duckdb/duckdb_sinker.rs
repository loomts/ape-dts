use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{
    rdb_query_builder::{RdbQueryBuilder, RdbQueryInfo},
    sinker::base_sinker::BaseSinker,
    sync_call_batch_fn, Sinker,
};

use anyhow::{bail, Context};
use dt_common::{
    error::Error,
    log_error,
    meta::{col_value::ColValue, duckdb::duckdb_meta_manager::DuckdbMetaManager},
    monitor::monitor::Monitor,
};

use dt_common::meta::{row_data::RowData, row_type::RowType};

use duckdb::{Connection, ToSql};

use async_trait::async_trait;

pub struct DuckdbSinker {
    pub conn: Option<Connection>,
    pub meta_manager: DuckdbMetaManager,
    pub batch_size: usize,
    pub monitor: Arc<Mutex<Monitor>>,
}

#[async_trait]
impl Sinker for DuckdbSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            self.serial_sink(&data)?;
        } else {
            match data[0].row_type {
                RowType::Insert => {
                    sync_call_batch_fn!(self, data, Self::batch_insert);
                }
                RowType::Delete => {
                    sync_call_batch_fn!(self, data, Self::batch_delete);
                }
                _ => self.serial_sink(&data)?,
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.meta_manager.close()?;
        if let Some(conn) = self.conn.take() {
            if let Err((_, err)) = conn.close() {
                bail!(Error::DuckdbError(err))
            }
        }
        Ok(())
    }
}

impl DuckdbSinker {
    fn serial_sink(&mut self, data: &[RowData]) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let mut data_size = 0;

        for row_data in data.iter() {
            data_size += row_data.data_size;

            let tb_meta = self
                .meta_manager
                .get_tb_meta(&data[0].schema, &data[0].tb)?;
            let query_builder = RdbQueryBuilder::new_for_duckdb(tb_meta, None);
            let query_info = query_builder.get_query_info(row_data, true)?;
            let none: Option<String> = Option::None;
            let params = Self::build_params(&query_info, &none);

            self.conn
                .as_ref()
                .unwrap()
                .execute(&query_info.sql, params.as_slice())
                .with_context(|| format!("serial sink failed, row_data: [{}]", row_data))?;
        }

        BaseSinker::update_serial_monitor(&mut self.monitor, data.len(), data_size, start_time)
    }

    fn batch_delete(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let data_size = self.batch_execute(
            data,
            start_index,
            batch_size,
            &format!(
                "batch delete failed, schema: {}, tb: {}",
                data[0].schema, &data[0].tb
            ),
        )?;
        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time)
    }

    fn batch_insert(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let data_size = match self.batch_execute(data, start_index, batch_size, "") {
            Ok(data_size) => data_size,
            Err(_) => {
                log_error!(
                    "batch insert failed, will delete first, then insert, schema: {}, tb: {}",
                    data[0].schema,
                    &data[0].tb
                );

                // build delete data
                let mut delete_data = vec![];
                for row_data in data.iter().skip(start_index).take(batch_size) {
                    delete_data.push(RowData::new(
                        row_data.schema.clone(),
                        row_data.tb.clone(),
                        RowType::Delete,
                        row_data.after.clone(),
                        None,
                    ));
                }

                // delete first
                self.batch_execute(
                    &mut delete_data,
                    0,
                    batch_size,
                    &format!(
                        "batch delete failed, schema: {}, tb: {}",
                        data[0].schema, &data[0].tb
                    ),
                )?;

                // then insert
                self.batch_execute(
                    data,
                    start_index,
                    batch_size,
                    &format!(
                        "batch insert failed even after delete, schema: {}, tb: {}",
                        data[0].schema, &data[0].tb
                    ),
                )?
            }
        };
        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time)
    }

    fn batch_execute(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
        context_info: &str,
    ) -> anyhow::Result<usize> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&data[0].schema, &data[0].tb)?;
        let query_builder = RdbQueryBuilder::new_for_duckdb(tb_meta, None);

        let (query_info, data_size) = if data[0].row_type == RowType::Delete {
            query_builder.get_batch_delete_query(data, start_index, batch_size)?
        } else {
            query_builder.get_batch_insert_query(data, start_index, batch_size)?
        };

        let none: Option<String> = Option::None;
        let params = Self::build_params(&query_info, &none);
        self.conn
            .as_ref()
            .unwrap()
            .execute(&query_info.sql, params.as_slice())
            .with_context(|| context_info.to_string())?;
        Ok(data_size)
    }

    fn build_params<'a>(
        query_info: &'a RdbQueryInfo,
        none: &'a Option<String>,
    ) -> Vec<&'a dyn ToSql> {
        let mut params: Vec<&dyn ToSql> = Vec::new();
        for col_value in query_info.binds.iter() {
            if let Some(v) = col_value {
                match v {
                    ColValue::Tiny(v) => params.push(v),
                    ColValue::UnsignedTiny(v) => params.push(v),
                    ColValue::Short(v) => params.push(v),
                    ColValue::UnsignedShort(v) => params.push(v),
                    ColValue::Long(v) => params.push(v),
                    ColValue::UnsignedLong(v) => params.push(v),
                    ColValue::LongLong(v) => params.push(v),
                    ColValue::UnsignedLongLong(v) => params.push(v),
                    ColValue::Float(v) => params.push(v),
                    ColValue::Double(v) => params.push(v),
                    ColValue::Decimal(v) => params.push(v),
                    ColValue::Time(v) => params.push(v),
                    ColValue::Date(v) => params.push(v),
                    ColValue::DateTime(v) => params.push(v),
                    ColValue::Timestamp(v) => params.push(v),
                    ColValue::Year(v) => params.push(v),
                    ColValue::String(v) => params.push(v),
                    ColValue::RawString(v) => params.push(v),
                    ColValue::Blob(v) => params.push(v),
                    ColValue::Bit(v) => params.push(v),
                    ColValue::Set(v) => params.push(v),
                    ColValue::Set2(v) => params.push(v),
                    ColValue::Enum(v) => params.push(v),
                    ColValue::Enum2(v) => params.push(v),
                    ColValue::Json(v) => params.push(v),
                    ColValue::Json2(v) => params.push(v),
                    _ => params.push(none),
                }
            } else {
                params.push(none);
            }
        }
        params
    }
}
