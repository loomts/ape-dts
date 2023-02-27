use std::{collections::HashMap, sync::atomic::AtomicBool};

use async_recursion::async_recursion;
use async_trait::async_trait;

use concurrent_queue::ConcurrentQueue;
use log::info;
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient,
    event::{
        event_data::EventData, event_header::EventHeader, row_event::RowEvent,
        table_map_event::TableMapEvent,
    },
};

use crate::{
    error::Error,
    extractor::rdb_filter::RdbFilter,
    meta::{
        col_value::ColValue, mysql::mysql_meta_manager::MysqlMetaManager, row_data::RowData,
        row_type::RowType,
    },
    task::task_util::TaskUtil,
    traits::traits::Extractor,
};

use super::mysql_col_value_convertor::MysqlColValueConvertor;

pub struct MysqlCdcExtractor<'a> {
    pub meta_manager: MysqlMetaManager,
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub filter: RdbFilter,
    pub url: String,
    pub binlog_filename: String,
    pub binlog_position: u32,
    pub server_id: u64,
    pub shut_down: &'a AtomicBool,
}

#[async_trait]
impl Extractor for MysqlCdcExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        self.extract_internal().await
    }
}

impl MysqlCdcExtractor<'_> {
    async fn extract_internal(&mut self) -> Result<(), Error> {
        info!(
            "start extracting binlog, binlog_filename: {}, binlog_position: {}",
            self.binlog_filename, self.binlog_position
        );

        let mut client = BinlogClient {
            url: self.url.clone(),
            binlog_filename: self.binlog_filename.clone(),
            binlog_position: self.binlog_position,
            server_id: self.server_id,
        };
        let mut stream = client.connect().await.unwrap();

        let mut table_map_event_map = HashMap::new();
        let mut binlog_filename = self.binlog_filename.clone();
        loop {
            let (header, data) = stream.read().await?;
            match data {
                EventData::Rotate(r) => {
                    binlog_filename = r.binlog_filename;
                }

                _ => {
                    self.parse_events(header, data, &binlog_filename, &mut table_map_event_map)
                        .await?
                }
            }
        }
    }

    #[async_recursion]
    async fn parse_events(
        &mut self,
        header: EventHeader,
        data: EventData,
        binlog_filename: &str,
        table_map_event_map: &mut HashMap<u64, TableMapEvent>,
    ) -> Result<(), Error> {
        let position = format!(
            "binlog_filename:{},next_event_position:{},timestamp:{}",
            binlog_filename, header.next_event_position, header.timestamp
        );

        match data {
            EventData::TableMap(d) => {
                table_map_event_map.insert(d.table_id, d);
            }

            EventData::TransactionPayload(event) => {
                for event in event.uncompressed_events {
                    self.parse_events(event.0, event.1, binlog_filename, table_map_event_map)
                        .await?;
                }
            }

            EventData::WriteRows(mut w) => {
                for event in w.rows.iter_mut() {
                    let table_map_event = table_map_event_map.get(&w.table_id).unwrap();
                    let col_values = self
                        .parse_row_data(&table_map_event, &w.included_columns, event)
                        .await?;
                    let row_data = RowData {
                        db: table_map_event.database_name.clone(),
                        tb: table_map_event.table_name.clone(),
                        row_type: RowType::Insert,
                        before: Option::None,
                        after: Some(col_values),
                        position: position.clone(),
                    };
                    let _ = self.push_row_to_buf(row_data).await?;
                }
            }

            EventData::UpdateRows(mut u) => {
                for event in u.rows.iter_mut() {
                    let table_map_event = table_map_event_map.get(&u.table_id).unwrap();
                    let col_values_before = self
                        .parse_row_data(&table_map_event, &u.included_columns_before, &mut event.0)
                        .await?;
                    let col_values_after = self
                        .parse_row_data(&table_map_event, &u.included_columns_after, &mut event.1)
                        .await?;
                    let row_data = RowData {
                        db: table_map_event.database_name.clone(),
                        tb: table_map_event.table_name.clone(),
                        row_type: RowType::Update,
                        before: Some(col_values_before),
                        after: Some(col_values_after),
                        position: position.clone(),
                    };
                    let _ = self.push_row_to_buf(row_data).await?;
                }
            }

            EventData::DeleteRows(mut d) => {
                for event in d.rows.iter_mut() {
                    let table_map_event = table_map_event_map.get(&d.table_id).unwrap();
                    let col_values = self
                        .parse_row_data(&table_map_event, &d.included_columns, event)
                        .await?;
                    let row_data = RowData {
                        db: table_map_event.database_name.clone(),
                        tb: table_map_event.table_name.clone(),
                        row_type: RowType::Delete,
                        before: Some(col_values),
                        after: Option::None,
                        position: position.clone(),
                    };
                    let _ = self.push_row_to_buf(row_data).await?;
                }
            }

            _ => {}
        }

        Ok(())
    }

    async fn push_row_to_buf(&mut self, row_data: RowData) -> Result<(), Error> {
        if self
            .filter
            .filter(&row_data.db, &row_data.tb, &row_data.row_type)
        {
            return Ok(());
        }

        while self.buffer.is_full() {
            TaskUtil::sleep_millis(1).await;
        }
        let _ = self.buffer.push(row_data);
        Ok(())
    }

    async fn parse_row_data(
        &mut self,
        table_map_event: &TableMapEvent,
        included_columns: &Vec<bool>,
        event: &mut RowEvent,
    ) -> Result<HashMap<String, ColValue>, Error> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&table_map_event.database_name, &table_map_event.table_name)
            .await?;

        if included_columns.len() != event.column_values.len() {
            return Err(Error::ColumnNotMatch);
        }

        let mut data = HashMap::new();
        for i in (0..tb_meta.cols.len()).rev() {
            let key = tb_meta.cols.get(i).unwrap();
            if let Some(false) = included_columns.get(i) {
                data.insert(key.clone(), ColValue::None);
                continue;
            }

            let meta = tb_meta.col_meta_map.get(key);
            let raw_value = event.column_values.remove(i);
            let value = MysqlColValueConvertor::from_binlog(&meta.unwrap(), raw_value);
            data.insert(key.clone(), value);
        }
        Ok(data)
    }
}
