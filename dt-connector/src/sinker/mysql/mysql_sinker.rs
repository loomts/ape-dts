use std::{str::FromStr, sync::Arc, time::Instant};

use crate::{
    call_batch_fn, close_conn_pool, rdb_query_builder::RdbQueryBuilder, rdb_router::RdbRouter,
    Sinker,
};

use async_rwlock::RwLock;
use dt_common::{
    error::Error,
    log_error, log_info,
    monitor::monitor::{CounterType, Monitor},
};

use dt_meta::{
    ddl_data::DdlData, ddl_type::DdlType, mysql::mysql_meta_manager::MysqlMetaManager,
    row_data::RowData, row_type::RowType,
};

use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    MySql, Pool, Transaction,
};

use async_trait::async_trait;

#[derive(Clone)]
pub struct MysqlSinker {
    pub url: String,
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub router: RdbRouter,
    pub batch_size: usize,
    pub monitor: Arc<RwLock<Monitor>>,
    pub transaction_command: String,
}

#[async_trait]
impl Sinker for MysqlSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            self.serial_sink(data).await.unwrap();
        } else {
            match data[0].row_type {
                RowType::Insert => {
                    call_batch_fn!(self, data, Self::batch_insert);
                }
                RowType::Delete => {
                    call_batch_fn!(self, data, Self::batch_delete);
                }
                _ => self.serial_sink(data).await.unwrap(),
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        return close_conn_pool!(self);
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        for ddl_data in data.iter() {
            log_info!("sink ddl: {}", ddl_data.query);
            let query = sqlx::query(&ddl_data.query);

            // create a tmp connection with databse since sqlx conn pool does NOT support `USE db`
            let mut conn_options = MySqlConnectOptions::from_str(&self.url).unwrap();
            if !ddl_data.schema.is_empty() && ddl_data.ddl_type != DdlType::CreateDatabase {
                conn_options = conn_options.database(&ddl_data.schema);
            }

            let conn_pool = MySqlPoolOptions::new()
                .max_connections(1)
                .connect_with(conn_options)
                .await
                .unwrap();
            query.execute(&conn_pool).await.unwrap();
        }
        Ok(())
    }

    async fn refresh_meta(&mut self, data: Vec<DdlData>) -> Result<(), Error> {
        for ddl_data in data.iter() {
            self.meta_manager
                .invalidate_cache(&ddl_data.schema, &ddl_data.tb);
        }
        Ok(())
    }

    fn get_monitor(&self) -> Option<Arc<RwLock<Monitor>>> {
        Some(self.monitor.clone())
    }
}

impl MysqlSinker {
    async fn serial_sink(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        if self.is_transaction_enable() {
            return self.transaction_serial_sink(data).await;
        }

        for row_data in data.iter() {
            let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&row_data).await?;
            let query_builder = RdbQueryBuilder::new_for_mysql(&tb_meta);

            let (mut sql, cols, binds) = query_builder.get_query_info(row_data)?;
            sql = self.handle_dialect(&sql);
            let query = query_builder.create_mysql_query(&sql, &cols, &binds);

            query.execute(&self.conn_pool).await.unwrap();
        }

        Ok(())
    }

    async fn transaction_serial_sink(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        let mut transaction = self.conn_pool.begin().await.unwrap();

        self.execute_transaction_command(&mut transaction).await;

        for row_data in data.iter() {
            let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&row_data).await?;
            let query_builder = RdbQueryBuilder::new_for_mysql(&tb_meta);

            let (mut sql, cols, binds) = query_builder.get_query_info(row_data)?;
            sql = self.handle_dialect(&sql);
            let query = query_builder.create_mysql_query(&sql, &cols, &binds);

            query.execute(&mut transaction).await.unwrap();
        }

        transaction.commit().await.unwrap();

        Ok(())
    }

    async fn batch_delete(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;
        let query_builder = RdbQueryBuilder::new_for_mysql(&tb_meta);

        let (sql, cols, binds) =
            query_builder.get_batch_delete_query(data, start_index, batch_size)?;
        let query = query_builder.create_mysql_query(&sql, &cols, &binds);

        let start_time = Instant::now();

        if self.is_transaction_enable() {
            let mut transaction = self.conn_pool.begin().await.unwrap();

            self.execute_transaction_command(&mut transaction).await;

            query.execute(&mut transaction).await.unwrap();

            transaction.commit().await.unwrap();
        } else {
            query.execute(&self.conn_pool).await.unwrap();
        }

        self.update_monitor(batch_size, start_time.elapsed().as_micros())
            .await;
        Ok(())
    }

    async fn batch_insert(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;
        let query_builder = RdbQueryBuilder::new_for_mysql(&tb_meta);

        let (mut sql, cols, binds) =
            query_builder.get_batch_insert_query(data, start_index, batch_size)?;
        sql = self.handle_dialect(&sql);
        let query = query_builder.create_mysql_query(&sql, &cols, &binds);

        let start_time = Instant::now();
        let execute_error: Option<sqlx::Error>;

        if self.is_transaction_enable() {
            let mut transaction = self.conn_pool.begin().await.unwrap();

            self.execute_transaction_command(&mut transaction).await;

            query.execute(&mut transaction).await.unwrap();

            execute_error = match transaction.commit().await {
                Err(e) => Some(e),
                _ => None,
            };
        } else {
            execute_error = match query.execute(&self.conn_pool).await {
                Err(e) => Some(e),
                _ => None,
            };
        }

        if let Some(error) = execute_error {
            log_error!(
                "batch insert failed, will insert one by one, schema: {}, tb: {}, error: {}",
                tb_meta.basic.schema,
                tb_meta.basic.tb,
                error.to_string()
            );
            // insert one by one
            let sub_data = &data[start_index..start_index + batch_size];
            self.serial_sink(sub_data.to_vec()).await.unwrap();
        }

        self.update_monitor(batch_size, start_time.elapsed().as_micros())
            .await;
        Ok(())
    }

    #[inline(always)]
    fn handle_dialect(&self, sql: &str) -> String {
        sql.replace("INSERT", "REPLACE")
    }

    async fn execute_transaction_command(&self, transaction: &mut Transaction<'_, MySql>) {
        sqlx::query(&self.transaction_command)
            .execute(transaction)
            .await
            .unwrap();
    }

    fn is_transaction_enable(&self) -> bool {
        !self.transaction_command.is_empty()
    }

    async fn update_monitor(&mut self, record_count: usize, rt: u128) {
        self.monitor
            .write()
            .await
            .add_counter(CounterType::RecordsPerQuery, record_count)
            .add_counter(CounterType::Records, record_count)
            .add_counter(CounterType::RtPerQuery, rt as usize);
    }
}
