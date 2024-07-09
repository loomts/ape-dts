use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use dt_common::meta::{
    ddl_data::DdlData, pg::pg_meta_manager::PgMetaManager, rdb_meta_manager::RdbMetaManager,
    row_data::RowData, struct_meta::statement::struct_statement::StructStatement,
};
use dt_common::{monitor::monitor::Monitor, rdb_filter::RdbFilter};
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
    pub reverse_router: RdbRouter,
    pub batch_size: usize,
    pub monitor: Arc<Mutex<Monitor>>,
    pub filter: RdbFilter,
}

#[async_trait]
impl Sinker for PgChecker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            self.serial_check(data).await?;
        } else {
            call_batch_fn!(self, data, Self::batch_check);
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.meta_manager.close().await?;
        self.extractor_meta_manager.close().await?;
        return close_conn_pool!(self);
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        self.serial_ddl_check(data).await?;
        Ok(())
    }
}

impl PgChecker {
    async fn serial_check(&mut self, data: Vec<RowData>) -> anyhow::Result<()> {
        let start_time = Instant::now();

        if data.is_empty() {
            return Ok(());
        }
        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;

        let mut miss = Vec::new();
        let mut diff = Vec::new();
        for src_row_data in data.iter() {
            let query_builder = RdbQueryBuilder::new_for_pg(tb_meta);
            let query_info = query_builder.get_select_query(src_row_data)?;
            let query = query_builder.create_pg_query(&query_info);

            let mut rows = query.fetch(&self.conn_pool);
            if let Some(row) = rows.try_next().await.unwrap() {
                let dst_row_data = RowData::from_pg_row(&row, tb_meta);
                let diff_col_values = BaseChecker::compare_row_data(src_row_data, &dst_row_data);
                if !diff_col_values.is_empty() {
                    let diff_log = BaseChecker::build_diff_log(
                        src_row_data,
                        diff_col_values,
                        &mut self.extractor_meta_manager,
                        &self.reverse_router,
                    )
                    .await?;
                    diff.push(diff_log);
                }
            } else {
                let miss_log = BaseChecker::build_miss_log(
                    src_row_data,
                    &mut self.extractor_meta_manager,
                    &self.reverse_router,
                )
                .await?;
                miss.push(miss_log);
            }
        }
        BaseChecker::log_dml(miss, diff);

        BaseSinker::update_serial_monitor(&mut self.monitor, data.len(), 0, start_time).await
    }

    async fn batch_check(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();

        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;
        let query_builder = RdbQueryBuilder::new_for_pg(tb_meta);

        // build fetch dst sql
        let query_info = query_builder.get_batch_select_query(data, start_index, batch_size)?;
        let query = query_builder.create_pg_query(&query_info);

        // fetch dst
        let mut dst_row_data_map = HashMap::new();
        let mut rows = query.fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_pg_row(&row, tb_meta);
            let hash_code = row_data.get_hash_code(&tb_meta.basic);
            dst_row_data_map.insert(hash_code, row_data);
        }

        let (miss, diff) = BaseChecker::batch_compare_row_datas(
            data,
            &dst_row_data_map,
            start_index,
            batch_size,
            &tb_meta.basic,
            &mut self.extractor_meta_manager,
            &self.reverse_router,
        )
        .await?;
        BaseChecker::log_dml(miss, diff);

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, 0, start_time).await
    }

    async fn serial_ddl_check(&mut self, mut data: Vec<DdlData>) -> anyhow::Result<()> {
        for src_data in data.iter_mut() {
            if src_data.statement.is_none() {
                continue;
            }

            let src_statement = src_data.statement.as_mut().unwrap();
            let schema = match src_statement {
                StructStatement::PgCreateSchema { statement } => statement.schema.name.clone(),
                StructStatement::PgCreateTable { statement } => statement.table.schema_name.clone(),
                _ => String::new(),
            };

            let mut struct_fetcher = PgStructFetcher {
                conn_pool: self.conn_pool.to_owned(),
                schema,
                filter: None,
            };

            let mut dst_statement = match &src_statement {
                StructStatement::PgCreateSchema { statement: _ } => {
                    let dst_statement = struct_fetcher.get_create_schema_statement().await?;
                    Some(StructStatement::PgCreateSchema {
                        statement: dst_statement,
                    })
                }

                StructStatement::PgCreateTable { statement } => {
                    let mut dst_statement = struct_fetcher
                        .get_create_table_statements(&statement.table.table_name)
                        .await?;
                    if dst_statement.is_empty() {
                        None
                    } else {
                        Some(StructStatement::PgCreateTable {
                            statement: dst_statement.remove(0),
                        })
                    }
                }

                _ => None,
            };

            BaseChecker::compare_struct(src_statement, &mut dst_statement, &self.filter)?;
        }
        Ok(())
    }
}
