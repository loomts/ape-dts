use async_recursion::async_recursion;
use async_trait::async_trait;
use sqlx::{mysql::MySqlArguments, query::Query, MySql, Pool};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use dt_meta::{
    adaptor::mysql_col_value_convertor::MysqlColValueConvertor, col_value::ColValue,
    dt_data::DtData, mysql::mysql_meta_manager::MysqlMetaManager, position::Position,
    row_data::RowData, row_type::RowType, syncer::Syncer,
};
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient,
    event::{
        event_data::EventData, event_header::EventHeader, query_event::QueryEvent,
        row_event::RowEvent, table_map_event::TableMapEvent,
    },
};

use dt_common::{
    config::config_enums::DbType,
    error::Error,
    log_error, log_info,
    utils::{rdb_filter::RdbFilter, time_util::TimeUtil},
};

use crate::{extractor::base_extractor::BaseExtractor, Extractor};

pub struct MysqlCdcExtractor {
    pub base_extractor: BaseExtractor,
    pub meta_manager: MysqlMetaManager,
    pub conn_pool: Pool<MySql>,
    pub filter: RdbFilter,
    pub url: String,
    pub binlog_filename: String,
    pub binlog_position: u32,
    pub server_id: u64,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_tb: String,
    pub syncer: Arc<Mutex<Syncer>>,
}

const QUERY_BEGIN: &str = "BEGIN";

#[async_trait]
impl Extractor for MysqlCdcExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "MysqlCdcExtractor starts, binlog_filename: {}, binlog_position: {}, heartbeat_interval_secs: {}, heartbeat_tb: {}",
            self.binlog_filename,
            self.binlog_position,
            self.heartbeat_interval_secs,
            self.heartbeat_tb
        );
        self.extract_internal().await
    }
}

impl MysqlCdcExtractor {
    async fn extract_internal(&mut self) -> Result<(), Error> {
        let mut client = BinlogClient {
            url: self.url.clone(),
            binlog_filename: self.binlog_filename.clone(),
            binlog_position: self.binlog_position,
            server_id: self.server_id,
        };
        let mut stream = client.connect().await.unwrap();
        let mut table_map_event_map = HashMap::new();
        let mut binlog_filename = self.binlog_filename.clone();

        // start heartbeat
        self.start_heartbeat().unwrap();

        loop {
            let (header, data) = stream.read().await.unwrap();
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
        let position = Position::MysqlCdc {
            // TODO, get server_id from source mysql
            server_id: String::new(),
            binlog_filename: binlog_filename.into(),
            next_event_position: header.next_event_position,
            timestamp: Position::format_timestamp_millis(header.timestamp as i64 * 1000),
        };

        match data {
            EventData::TableMap(d) => {
                table_map_event_map.insert(d.table_id, d);
            }

            EventData::TransactionPayload(event) => {
                for (mut inner_header, data) in event.uncompressed_events {
                    // headers of uncompressed events have no next_event_position,
                    // use header of TransactionPayload instead
                    inner_header.next_event_position = header.next_event_position;
                    self.parse_events(inner_header, data, binlog_filename, table_map_event_map)
                        .await?;
                }
            }

            EventData::WriteRows(mut w) => {
                for event in w.rows.iter_mut() {
                    let table_map_event = table_map_event_map.get(&w.table_id).unwrap();
                    if self.filter_event(table_map_event, RowType::Insert) {
                        continue;
                    }

                    let col_values = self
                        .parse_row_data(table_map_event, &w.included_columns, event)
                        .await?;
                    let row_data = RowData::new(
                        table_map_event.database_name.clone(),
                        table_map_event.table_name.clone(),
                        RowType::Insert,
                        None,
                        Some(col_values),
                    );
                    self.push_row_to_buf(row_data, position.clone()).await?;
                }
            }

            EventData::UpdateRows(mut u) => {
                for event in u.rows.iter_mut() {
                    let table_map_event = table_map_event_map.get(&u.table_id).unwrap();
                    if self.filter_event(table_map_event, RowType::Update) {
                        continue;
                    }

                    let col_values_before = self
                        .parse_row_data(table_map_event, &u.included_columns_before, &mut event.0)
                        .await?;
                    let col_values_after = self
                        .parse_row_data(table_map_event, &u.included_columns_after, &mut event.1)
                        .await?;
                    let row_data = RowData::new(
                        table_map_event.database_name.clone(),
                        table_map_event.table_name.clone(),
                        RowType::Update,
                        Some(col_values_before),
                        Some(col_values_after),
                    );
                    self.push_row_to_buf(row_data, position.clone()).await?;
                }
            }

            EventData::DeleteRows(mut d) => {
                for event in d.rows.iter_mut() {
                    let table_map_event = table_map_event_map.get(&d.table_id).unwrap();
                    if self.filter_event(table_map_event, RowType::Delete) {
                        continue;
                    }

                    let col_values = self
                        .parse_row_data(table_map_event, &d.included_columns, event)
                        .await?;
                    let row_data = RowData::new(
                        table_map_event.database_name.clone(),
                        table_map_event.table_name.clone(),
                        RowType::Delete,
                        Some(col_values),
                        None,
                    );
                    self.push_row_to_buf(row_data, position.clone()).await?;
                }
            }

            EventData::Query(query) => {
                self.handle_query_event(query, position.clone()).await?;
            }

            EventData::Xid(xid) => {
                let commit = DtData::Commit {
                    xid: xid.xid.to_string(),
                };
                self.base_extractor
                    .push_dt_data(commit, position.clone())
                    .await?;
            }

            _ => {}
        }

        Ok(())
    }

    async fn push_row_to_buf(
        &mut self,
        row_data: RowData,
        position: Position,
    ) -> Result<(), Error> {
        self.base_extractor.push_row(row_data, position).await
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
            return Err(Error::ExtractorError(
                "included_columns not match column_values in binlog".into(),
            ));
        }

        let mut data = HashMap::new();
        for i in (0..tb_meta.basic.cols.len()).rev() {
            let key = tb_meta.basic.cols.get(i).unwrap();
            if let Some(false) = included_columns.get(i) {
                data.insert(key.clone(), ColValue::None);
                continue;
            }

            let col_type = tb_meta.col_type_map.get(key).unwrap();
            let raw_value = event.column_values.remove(i);
            let value = MysqlColValueConvertor::from_binlog(col_type, raw_value)?;
            data.insert(key.clone(), value);
        }
        Ok(data)
    }

    async fn handle_query_event(
        &mut self,
        query: QueryEvent,
        position: Position,
    ) -> Result<(), Error> {
        // TODO, currently we do not parse ddl if filtered,
        // but we should always try to parse ddl in the future
        if self.filter.filter_all_ddl() {
            return Ok(());
        }

        if query.query == QUERY_BEGIN {
            return Ok(());
        }

        log_info!("received ddl: {:?}", query);
        if let Ok(ddl_data) = self
            .base_extractor
            .parse_ddl(&query.schema, &query.query)
            .await
        {
            // invalidate metadata cache
            self.meta_manager
                .invalidate_cache(&ddl_data.schema, &ddl_data.tb);

            if !self.filter.filter_ddl(
                &ddl_data.schema,
                &ddl_data.tb,
                &ddl_data.ddl_type.to_string(),
            ) {
                self.base_extractor
                    .push_dt_data(DtData::Ddl { ddl_data }, position)
                    .await
                    .unwrap();
            }
        }
        Ok(())
    }

    fn filter_event(&mut self, table_map_event: &TableMapEvent, row_type: RowType) -> bool {
        let db = &table_map_event.database_name;
        let tb = &table_map_event.table_name;
        let filtered = self.filter.filter_event(db, tb, &row_type.to_string());
        if filtered {
            return !self.base_extractor.is_data_marker_info(db, tb);
        }
        filtered
    }

    fn start_heartbeat(&self) -> Result<(), Error> {
        let db_tb = self.base_extractor.precheck_heartbeat(
            self.heartbeat_interval_secs,
            &self.heartbeat_tb,
            DbType::Pg,
        );
        if db_tb.len() != 2 {
            return Ok(());
        }

        let (server_id, heartbeat_interval_secs, syncer, conn_pool) = (
            self.server_id,
            self.heartbeat_interval_secs,
            self.syncer.clone(),
            self.conn_pool.clone(),
        );

        tokio::spawn(async move {
            let mut start_time = Instant::now();
            loop {
                if start_time.elapsed().as_secs() >= heartbeat_interval_secs {
                    Self::heartbeat(server_id, &db_tb[0], &db_tb[1], &syncer, &conn_pool)
                        .await
                        .unwrap();
                    start_time = Instant::now();
                }
                TimeUtil::sleep_millis(1000 * heartbeat_interval_secs).await;
            }
        });
        log_info!("heartbeat started");
        Ok(())
    }

    async fn heartbeat(
        server_id: u64,
        db: &str,
        tb: &str,
        syncer: &Arc<Mutex<Syncer>>,
        conn_pool: &Pool<MySql>,
    ) -> Result<(), Error> {
        let (received_binlog_filename, received_next_event_position, received_timestamp) =
            if let Position::MysqlCdc {
                binlog_filename,
                next_event_position,
                timestamp,
                ..
            } = &syncer.lock().unwrap().received_position
            {
                (
                    binlog_filename.to_owned(),
                    *next_event_position,
                    timestamp.to_owned(),
                )
            } else {
                (String::new(), 0, String::new())
            };

        let (flushed_binlog_filename, flushed_next_event_position, flushed_timestamp) =
            if let Position::MysqlCdc {
                binlog_filename,
                next_event_position,
                timestamp,
                ..
            } = &syncer.lock().unwrap().committed_position
            {
                (
                    binlog_filename.to_owned(),
                    *next_event_position,
                    timestamp.to_owned(),
                )
            } else {
                (String::new(), 0, String::new())
            };

        // CREATE TABLE test_db_1.ape_dts_heartbeat(
        //     server_id INT UNSIGNED,
        //     update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        //     received_binlog_filename VARCHAR(255),
        //     received_next_event_position INT UNSIGNED,
        //     received_timestamp VARCHAR(255),
        //     flushed_binlog_filename VARCHAR(255),
        //     flushed_next_event_position INT UNSIGNED,
        //     flushed_timestamp VARCHAR(255),
        //     PRIMARY KEY(server_id)
        // );
        let sql = format!(
            "REPLACE INTO `{}`.`{}` (server_id, update_timestamp, 
                received_binlog_filename, received_next_event_position, received_timestamp, 
                flushed_binlog_filename, flushed_next_event_position, flushed_timestamp) 
            VALUES ({}, now(), '{}', {}, '{}', '{}', {}, '{}')",
            db,
            tb,
            server_id,
            received_binlog_filename,
            received_next_event_position,
            received_timestamp,
            flushed_binlog_filename,
            flushed_next_event_position,
            flushed_timestamp,
        );

        let query: Query<MySql, MySqlArguments> = sqlx::query(&sql);
        if let Err(err) = query.execute(conn_pool).await {
            log_error!("heartbeat failed: {:?}", err);
        }
        Ok(())
    }
}
