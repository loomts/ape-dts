use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::StreamExt;

use postgres_protocol::message::backend::{
    DeleteBody, InsertBody,
    LogicalReplicationMessage::{
        Begin, Commit, Delete, Insert, Origin, Relation, Truncate, Type, Update,
    },
    RelationBody,
    ReplicationMessage::*,
    TupleData, UpdateBody,
};

use postgres_types::PgLsn;
use tokio_postgres::replication::LogicalReplicationStream;

use dt_common::{error::Error, log_error, log_info, utils::rdb_filter::RdbFilter};

use crate::{
    extractor::{base_extractor::BaseExtractor, pg::pg_cdc_client::PgCdcClient},
    Extractor,
};
use dt_meta::{
    adaptor::pg_col_value_convertor::PgColValueConvertor,
    col_value::ColValue,
    dt_data::DtData,
    pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
    position::Position,
    rdb_tb_meta::RdbTbMeta,
    row_data::RowData,
    row_type::RowType,
    syncer::Syncer,
};

pub struct PgCdcExtractor {
    pub base_extractor: BaseExtractor,
    pub meta_manager: PgMetaManager,
    pub filter: RdbFilter,
    pub url: String,
    pub slot_name: String,
    pub pub_name: String,
    pub start_lsn: String,
    pub heartbeat_interval_secs: u64,
    pub syncer: Arc<Mutex<Syncer>>,
}

const SECS_FROM_1970_TO_2000: i64 = 946_684_800;

#[async_trait]
impl Extractor for PgCdcExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "PgCdcExtractor starts, slot_name: {}, start_lsn: {}, heartbeat_interval_secs: {}",
            self.slot_name,
            self.start_lsn,
            self.heartbeat_interval_secs
        );
        self.extract_internal().await.unwrap();
        Ok(())
    }
}

impl PgCdcExtractor {
    async fn extract_internal(&mut self) -> Result<(), Error> {
        let mut cdc_client = PgCdcClient {
            url: self.url.clone(),
            pub_name: self.pub_name.clone(),
            slot_name: self.slot_name.clone(),
            start_lsn: self.start_lsn.clone(),
        };
        let (stream, actual_start_lsn) = cdc_client.connect().await?;
        tokio::pin!(stream);

        let mut last_tx_end_lsn = actual_start_lsn.clone();
        let mut xid = String::new();
        let mut start_time = Instant::now();

        let get_position = |lsn: &str, timestamp: i64| -> Position {
            Position::PgCdc {
                lsn: lsn.into(),
                timestamp: Position::format_timestamp_millis(
                    timestamp / 1000 + SECS_FROM_1970_TO_2000 * 1000,
                ),
            }
        };
        let mut position: Position = get_position("", 0);

        // refer: https://www.postgresql.org/docs/10/protocol-replication.html to get WAL data details
        loop {
            // send a heartbeat to keep alive
            if start_time.elapsed().as_secs() > self.heartbeat_interval_secs {
                self.heartbeat(&mut stream, &actual_start_lsn).await?;
                start_time = Instant::now();
            }

            match stream.next().await {
                Some(Ok(XLogData(body))) => {
                    let data = body.into_data();
                    match data {
                        Relation(relation) => {
                            self.decode_relation(&relation).await?;
                        }

                        // do not push Begin and Commit into buffer to accelerate sinking
                        Begin(begin) => {
                            position = get_position(&last_tx_end_lsn, begin.timestamp());
                            xid = begin.xid().to_string();
                        }

                        Commit(commit) => {
                            last_tx_end_lsn = PgLsn::from(commit.end_lsn()).to_string();
                            position = get_position(&last_tx_end_lsn, commit.timestamp());
                            let commit = DtData::Commit { xid: xid.clone() };
                            self.base_extractor
                                .push_dt_data(commit, position.clone())
                                .await?;
                        }

                        Origin(_origin) => {}

                        Truncate(_truncate) => {}

                        Type(_typee) => {}

                        Insert(insert) => {
                            self.decode_insert(&insert, &position).await?;
                        }

                        Update(update) => {
                            self.decode_update(&update, &position).await?;
                        }

                        Delete(delete) => {
                            self.decode_delete(&delete, &position).await?;
                        }

                        _ => {}
                    }
                }

                Some(Ok(PrimaryKeepAlive(data))) => {
                    // Send a standby status update and require a keep alive response
                    if data.reply() == 1 {
                        self.heartbeat(&mut stream, &actual_start_lsn).await?;
                        start_time = Instant::now();
                    }
                }

                Some(Ok(data)) => {
                    log_info!("received unkown replication data: {:?}", data);
                }

                Some(Err(error)) => panic!("unexpected replication stream error: {}", error),

                None => panic!("unexpected replication stream end"),
            }
        }
    }

    async fn heartbeat(
        &mut self,
        stream: &mut Pin<&mut LogicalReplicationStream>,
        start_lsn: &str,
    ) -> Result<(), Error> {
        let lsn =
            if let Position::PgCdc { lsn, .. } = &self.syncer.lock().unwrap().checkpoint_position {
                if lsn.is_empty() {
                    start_lsn.parse().unwrap()
                } else {
                    lsn.parse().unwrap()
                }
            } else {
                start_lsn.parse().unwrap()
            };

        // Postgres epoch is 2000-01-01T00:00:00Z
        let pg_epoch = UNIX_EPOCH + Duration::from_secs(SECS_FROM_1970_TO_2000 as u64);
        let ts = pg_epoch.elapsed().unwrap().as_micros() as i64;
        let result = stream
            .as_mut()
            .standby_status_update(lsn, lsn, lsn, ts, 1)
            .await;
        if let Err(error) = result {
            log_error!(
                "heartbeat to postgres failed, lsn: {}, error: {}",
                lsn.to_string(),
                error
            );
        }
        Ok(())
    }

    async fn decode_relation(&mut self, event: &RelationBody) -> Result<(), Error> {
        let schema = event.namespace()?;
        let tb = event.name()?;
        // if the tb is filtered, we won't try to get the tb_meta since we may get privilege errors,
        // but we need to keep the oid —— tb_meta map which may be used for decoding events.
        if self.filter.filter_tb(schema, tb) {
            let tb_meta = Self::mock_pg_tb_meta(schema, tb, event.rel_id() as i32);
            self.meta_manager
                .update_tb_meta_by_oid(event.rel_id() as i32, tb_meta)?;
            return Ok(());
        }

        // todo, use event.rel_id()
        let mut tb_meta = self.meta_manager.get_tb_meta(schema, tb).await?.to_owned();
        let mut col_names = Vec::new();
        for column in event.columns() {
            // todo: check type_id in oid_to_type
            let col_type = self
                .meta_manager
                .type_registry
                .oid_to_type
                .get(&column.type_id())
                .unwrap();
            let col_name = column.name()?;
            // update meta
            tb_meta
                .col_type_map
                .insert(col_name.to_string(), col_type.clone());

            col_names.push(col_name.to_string());
        }

        // align the column order of tb_meta to that of the wal log
        tb_meta.basic.cols = col_names;
        self.meta_manager
            .update_tb_meta_by_oid(event.rel_id() as i32, tb_meta)?;
        Ok(())
    }

    async fn decode_insert(
        &mut self,
        event: &InsertBody,
        position: &Position,
    ) -> Result<(), Error> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_oid(event.rel_id() as i32)?;
        if self.filter_event(&tb_meta, RowType::Insert) {
            return Ok(());
        }

        let col_values = self.parse_row_data(&tb_meta, event.tuple().tuple_data())?;
        let row_data = RowData::new(
            tb_meta.basic.schema,
            tb_meta.basic.tb,
            RowType::Insert,
            None,
            Some(col_values),
        );
        self.push_row_to_buf(row_data, position.clone()).await
    }

    async fn decode_update(
        &mut self,
        event: &UpdateBody,
        position: &Position,
    ) -> Result<(), Error> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_oid(event.rel_id() as i32)?;
        if self.filter_event(&tb_meta, RowType::Update) {
            return Ok(());
        }

        let basic = &tb_meta.basic;
        let col_values_after = self.parse_row_data(&tb_meta, event.new_tuple().tuple_data())?;
        let col_values_before = if let Some(old_tuple) = event.old_tuple() {
            self.parse_row_data(&tb_meta, old_tuple.tuple_data())?
        } else if let Some(key_tuple) = event.key_tuple() {
            self.parse_row_data(&tb_meta, key_tuple.tuple_data())?
        } else if !basic.id_cols.is_empty() {
            let mut col_values_tmp = HashMap::new();
            for col in basic.id_cols.iter() {
                col_values_tmp.insert(col.to_string(), col_values_after.get(col).unwrap().clone());
            }
            col_values_tmp
        } else {
            HashMap::new()
        };

        let row_data = RowData::new(
            basic.schema.clone(),
            basic.tb.clone(),
            RowType::Update,
            Some(col_values_before),
            Some(col_values_after),
        );
        self.push_row_to_buf(row_data, position.clone()).await
    }

    async fn decode_delete(
        &mut self,
        event: &DeleteBody,
        position: &Position,
    ) -> Result<(), Error> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_oid(event.rel_id() as i32)?;
        if self.filter_event(&tb_meta, RowType::Delete) {
            return Ok(());
        }

        let col_values = if let Some(old_tuple) = event.old_tuple() {
            self.parse_row_data(&tb_meta, old_tuple.tuple_data())?
        } else if let Some(key_tuple) = event.key_tuple() {
            self.parse_row_data(&tb_meta, key_tuple.tuple_data())?
        } else {
            HashMap::new()
        };

        let row_data = RowData::new(
            tb_meta.basic.schema,
            tb_meta.basic.tb,
            RowType::Delete,
            Some(col_values),
            None,
        );
        self.push_row_to_buf(row_data, position.clone()).await
    }

    fn parse_row_data(
        &mut self,
        tb_meta: &PgTbMeta,
        tuple_data: &[TupleData],
    ) -> Result<HashMap<String, ColValue>, Error> {
        let mut col_values: HashMap<String, ColValue> = HashMap::new();
        for i in 0..tuple_data.len() {
            let tuple_data = &tuple_data[i];
            let col = &tb_meta.basic.cols[i];
            let col_type = tb_meta.col_type_map.get(col).unwrap();

            match tuple_data {
                TupleData::Null => {
                    col_values.insert(col.to_string(), ColValue::None);
                }

                TupleData::Text(value) => {
                    let col_value =
                        PgColValueConvertor::from_wal(col_type, value, &mut self.meta_manager)?;
                    col_values.insert(col.to_string(), col_value);
                }

                TupleData::UnchangedToast => {
                    return Err(Error::ExtractorError(
                        "unexpected UnchangedToast value received".into(),
                    ))
                }
            }
        }
        Ok(col_values)
    }

    async fn push_row_to_buf(
        &mut self,
        row_data: RowData,
        position: Position,
    ) -> Result<(), Error> {
        self.base_extractor.push_row(row_data, position).await
    }

    fn filter_event(&mut self, tb_meta: &PgTbMeta, row_type: RowType) -> bool {
        self.filter.filter_event(
            &tb_meta.basic.schema,
            &tb_meta.basic.tb,
            &row_type.to_string(),
        )
    }

    fn mock_pg_tb_meta(schema: &str, tb: &str, oid: i32) -> PgTbMeta {
        PgTbMeta {
            basic: RdbTbMeta {
                schema: schema.into(),
                tb: tb.into(),
                ..Default::default()
            },
            oid,
            col_type_map: HashMap::new(),
        }
    }
}
