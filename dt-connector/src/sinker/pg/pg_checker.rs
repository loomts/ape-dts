use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use dt_common::{error::Error, monitor::monitor::Monitor, utils::rdb_filter::RdbFilter};
use dt_meta::{
    ddl_data::DdlData, pg::pg_meta_manager::PgMetaManager, rdb_meta_manager::RdbMetaManager,
    row_data::RowData, struct_meta::statement::struct_statement::StructStatement,
};
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};

use crate::{
    call_batch_fn, close_conn_pool,
    meta_fetcher::pg::pg_struct_fetcher::PgStructFetcher,
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    sinker::{base_checker::BaseChecker, base_sinker::BaseSinker},
    Sinker,
};

#[derive(Clone)]
pub struct PgChecker {
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub extractor_meta_manager: RdbMetaManager,
    pub router: RdbRouter,
    pub batch_size: usize,
    pub monitor: Arc<Mutex<Monitor>>,
    pub filter: RdbFilter,
}

#[async_trait]
impl Sinker for PgChecker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            self.serial_check(data).await.unwrap();
        } else {
            call_batch_fn!(self, data, Self::batch_check);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        return close_conn_pool!(self);
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        self.serial_ddl_check(data).await.unwrap();
        Ok(())
    }
}

impl PgChecker {
    async fn serial_check(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        let start_time = Instant::now();

        if data.is_empty() {
            return Ok(());
        }
        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;

        let mut miss = Vec::new();
        let mut diff = Vec::new();
        for row_data_src in data.iter() {
            let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta);
            let (sql, cols, binds) = query_builder.get_select_query(row_data_src)?;
            let query = query_builder.create_pg_query(&sql, &cols, &binds);

            let mut rows = query.fetch(&self.conn_pool);
            if let Some(row) = rows.try_next().await.unwrap() {
                let row_data_dst = RowData::from_pg_row(&row, &tb_meta);
                if !BaseChecker::compare_row_data(row_data_src, &row_data_dst) {
                    diff.push(row_data_src.to_owned());
                }
            } else {
                miss.push(row_data_src.to_owned());
            }
        }
        BaseChecker::log_dml(&mut self.extractor_meta_manager, &self.router, miss, diff)
            .await
            .unwrap();

        BaseSinker::update_serial_monitor(&mut self.monitor, data.len(), start_time).await
    }

    async fn batch_check(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let start_time = Instant::now();

        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;
        let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta);

        // build fetch dst sql
        let (sql, cols, binds) =
            query_builder.get_batch_select_query(data, start_index, batch_size)?;
        let query = query_builder.create_pg_query(&sql, &cols, &binds);

        // fetch dst
        let mut dst_row_data_map = HashMap::new();
        let mut rows = query.fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_pg_row(&row, &tb_meta);
            let hash_code = row_data.get_hash_code(&tb_meta.basic);
            dst_row_data_map.insert(hash_code, row_data);
        }

        let (miss, diff) = BaseChecker::batch_compare_row_datas(
            data,
            &dst_row_data_map,
            &tb_meta.basic,
            start_index,
            batch_size,
        );
        BaseChecker::log_dml(&mut self.extractor_meta_manager, &self.router, miss, diff)
            .await
            .unwrap();

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, start_time).await
    }

    async fn serial_ddl_check(&mut self, mut data: Vec<DdlData>) -> Result<(), Error> {
        for data_src in data.iter_mut() {
            if data_src.statement.is_none() {
                continue;
            }

            let mut src_statement = data_src.statement.as_mut().unwrap();
            let schema = match src_statement {
                StructStatement::PgCreateDatabase { statement } => statement.database.name.clone(),
                StructStatement::PgCreateTable { statement } => statement.table.schema_name.clone(),
                _ => String::new(),
            };

            let mut struct_fetcher = PgStructFetcher {
                conn_pool: self.conn_pool.to_owned(),
                schema,
                filter: None,
            };

            let mut dst_statement = match &src_statement {
                StructStatement::PgCreateDatabase { statement: _ } => {
                    let dst_statement = struct_fetcher
                        .get_create_database_statement()
                        .await
                        .unwrap();
                    Some(StructStatement::PgCreateDatabase {
                        statement: dst_statement,
                    })
                }

                StructStatement::PgCreateTable { statement } => {
                    let mut dst_statement = struct_fetcher
                        .get_create_table_statements(&statement.table.table_name)
                        .await
                        .unwrap();
                    if dst_statement.len() == 0 {
                        None
                    } else {
                        Some(StructStatement::PgCreateTable {
                            statement: dst_statement.remove(0),
                        })
                    }
                }

                _ => None,
            };

            BaseChecker::compare_struct(&mut src_statement, &mut dst_statement, &self.filter)
                .unwrap();
        }
        Ok(())
    }
}
