use std::{collections::HashMap, sync::Arc, time::Instant};

use async_rwlock::RwLock;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};

use crate::{
    call_batch_fn, close_conn_pool,
    meta_fetcher::mysql::mysql_struct_fetcher::MysqlStructFetcher,
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    sinker::{base_checker::BaseChecker, base_sinker::BaseSinker},
    Sinker,
};

use dt_common::{error::Error, monitor::monitor::Monitor};

use dt_meta::{
    ddl_data::DdlData, mysql::mysql_meta_manager::MysqlMetaManager,
    rdb_meta_manager::RdbMetaManager, row_data::RowData,
    struct_meta::statement::struct_statement::StructStatement,
};

#[derive(Clone)]
pub struct MysqlChecker {
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub extractor_meta_manager: RdbMetaManager,
    pub router: RdbRouter,
    pub batch_size: usize,
    pub monitor: Arc<RwLock<Monitor>>,
}

#[async_trait]
impl Sinker for MysqlChecker {
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

    fn get_monitor(&self) -> Option<Arc<RwLock<Monitor>>> {
        Some(self.monitor.clone())
    }
}

impl MysqlChecker {
    async fn serial_check(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        let start_time = Instant::now();

        if data.is_empty() {
            return Ok(());
        }
        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;

        let mut miss = Vec::new();
        let mut diff = Vec::new();
        for row_data_src in data.iter() {
            let query_builder = RdbQueryBuilder::new_for_mysql(&tb_meta);
            let (sql, cols, binds) = query_builder.get_select_query(row_data_src)?;
            let query = query_builder.create_mysql_query(&sql, &cols, &binds);

            let mut rows = query.fetch(&self.conn_pool);
            if let Some(row) = rows.try_next().await.unwrap() {
                let row_data_dst = RowData::from_mysql_row(&row, &tb_meta);
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
        let query_builder = RdbQueryBuilder::new_for_mysql(&tb_meta);

        // build fetch dst sql
        let (sql, cols, binds) =
            query_builder.get_batch_select_query(data, start_index, batch_size)?;
        let query = query_builder.create_mysql_query(&sql, &cols, &binds);

        // fetch dst
        let mut dst_row_data_map = HashMap::new();
        let mut rows = query.fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_mysql_row(&row, &tb_meta);
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
            let db = match src_statement {
                StructStatement::MysqlCreateDatabase { statement } => {
                    statement.database.name.clone()
                }
                StructStatement::MysqlCreateTable { statement } => {
                    statement.table.database_name.clone()
                }
                _ => String::new(),
            };

            let mut struct_fetcher = MysqlStructFetcher {
                conn_pool: self.conn_pool.to_owned(),
                db: db.into(),
                filter: None,
                meta_manager: self.meta_manager.clone(),
            };

            let mut dst_statement = match &src_statement {
                StructStatement::MysqlCreateDatabase { statement: _ } => {
                    let dst_statement = struct_fetcher
                        .get_create_database_statement()
                        .await
                        .unwrap();
                    Some(StructStatement::MysqlCreateDatabase {
                        statement: dst_statement,
                    })
                }

                StructStatement::MysqlCreateTable { statement } => {
                    let mut dst_statement = struct_fetcher
                        .get_create_table_statements(&statement.table.table_name)
                        .await
                        .unwrap();
                    if dst_statement.len() == 0 {
                        None
                    } else {
                        Some(StructStatement::MysqlCreateTable {
                            statement: dst_statement.remove(0),
                        })
                    }
                }

                _ => None,
            };

            BaseChecker::compare_struct(&mut src_statement, &mut dst_statement).unwrap();
        }
        Ok(())
    }
}
