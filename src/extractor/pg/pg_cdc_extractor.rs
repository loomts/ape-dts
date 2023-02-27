use std::{
    collections::HashMap,
    sync::atomic::AtomicBool,
    time::{Duration, UNIX_EPOCH},
};

use async_trait::async_trait;

use concurrent_queue::ConcurrentQueue;
use futures::StreamExt;

use postgres_protocol::message::backend::{
    DeleteBody, InsertBody,
    LogicalReplicationMessage::{Begin, Commit, Insert, Relation},
    RelationBody,
    ReplicationMessage::*,
    TupleData, UpdateBody,
};

use postgres_types::PgLsn;

use crate::{
    error::Error,
    extractor::{pg::pg_cdc_client::PgCdcClient, rdb_filter::RdbFilter},
    meta::{
        col_value::ColValue, pg::pg_meta_manager::PgMetaManager, row_data::RowData,
        row_type::RowType,
    },
    task::task_util::TaskUtil,
    traits::traits::Extractor,
};

use super::pg_col_value_convertor::PgColValueConvertor;

pub struct PgCdcExtractor<'a> {
    pub meta_manager: PgMetaManager,
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub filter: RdbFilter,
    pub url: String,
    pub slot_name: String,
    pub start_sln: String,
    pub shut_down: &'a AtomicBool,
}

#[async_trait]
impl Extractor for PgCdcExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        Ok(self.extract_internal().await.unwrap())
    }
}

impl PgCdcExtractor<'_> {
    pub async fn extract_internal(&mut self) -> Result<(), Error> {
        let mut cdc_client = PgCdcClient {
            url: self.url.clone(),
            slot_name: self.slot_name.clone(),
            start_sln: self.start_sln.clone(),
        };
        let stream = cdc_client.connect().await?;
        tokio::pin!(stream);

        // refer: https://www.postgresql.org/docs/10/protocol-replication.html to get WAL data details
        loop {
            match stream.next().await {
                Some(Ok(XLogData(body))) => {
                    let data = body.into_data();
                    match data {
                        Relation(relation) => {
                            println!("relation: {:?}", relation);
                            self.decode_relation(&relation).await?;
                        }

                        Begin(begin) => {
                            println!("begin: {:?}", begin);
                        }

                        Insert(insert) => {
                            println!("insert: {:?}", insert);
                            self.decode_insert(&insert).await?;
                        }

                        Commit(commit) => {
                            println!("commit: {:?}", commit);
                        }

                        _ => {
                            println!("other XLogData: {:?}", data);
                        }
                    }
                }

                Some(Ok(PrimaryKeepAlive(data))) => {
                    // Send a standby status update and require a keep alive response
                    if data.reply() == 1 {
                        let lsn: PgLsn = self.start_sln.parse().unwrap();
                        // Postgres epoch is 2000-01-01T00:00:00Z
                        let pg_epoch = UNIX_EPOCH + Duration::from_secs(946_684_800);
                        let ts = pg_epoch.elapsed().unwrap().as_micros() as i64;
                        stream
                            .as_mut()
                            .standby_status_update(lsn, lsn, lsn, ts, 1)
                            .await
                            .unwrap();
                    }
                }

                Some(Ok(data)) => {
                    println!("other OK: {:?}", data);
                }

                Some(Err(x)) => panic!("unexpected replication stream error: {}", x),

                None => panic!("unexpected replication stream end"),
            }
        }
    }

    async fn decode_relation(&mut self, event: &RelationBody) -> Result<(), Error> {
        // todo, use event.rel_id()
        let mut tb_meta = self
            .meta_manager
            .get_tb_meta(event.namespace()?, event.name()?)
            .await?;

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
        tb_meta.cols = col_names;
        self.meta_manager
            .update_tb_meta_by_oid(event.rel_id() as i32, tb_meta)?;
        Ok(())
    }

    async fn decode_insert(&mut self, event: &InsertBody) -> Result<(), Error> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_oid(event.rel_id() as i32)?;
        let mut col_values: HashMap<String, ColValue> = HashMap::new();

        for i in 0..event.tuple().tuple_data().len() {
            let tuple_data = &event.tuple().tuple_data()[i];
            let col = &tb_meta.cols[i];
            let col_type = tb_meta.col_type_map.get(col).unwrap();

            match tuple_data {
                TupleData::Null => {}
                TupleData::Text(value) => {
                    let col_value = PgColValueConvertor::from_wal(&col_type, &value)?;
                    col_values.insert(col.to_string(), col_value);
                }
                TupleData::UnchangedToast => {}
            }
        }

        let row_data = RowData {
            db: tb_meta.schema,
            tb: tb_meta.tb,
            row_type: RowType::Insert,
            before: Option::None,
            after: Some(col_values),
            position: "".to_string(),
        };
        self.push_row_to_buf(row_data).await
    }

    async fn decode_update(
        &self,
        event: &UpdateBody,
        meta_manager: &mut PgMetaManager,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn decode_delete(
        &self,
        event: &DeleteBody,
        meta_manager: &mut PgMetaManager,
    ) -> Result<(), Error> {
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
}
