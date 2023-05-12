use std::sync::atomic::AtomicBool;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use futures::TryStreamExt;

use sqlx::{MySql, Pool};

use dt_common::{
    adaptor::{mysql_col_value_convertor::MysqlColValueConvertor, sqlx_ext::SqlxMysqlExt},
    error::Error,
    log_info,
    meta::{
        col_value::ColValue,
        dt_data::DtData,
        mysql::{
            mysql_col_type::MysqlColType, mysql_meta_manager::MysqlMetaManager,
            mysql_tb_meta::MysqlTbMeta,
        },
        row_data::RowData,
    },
    utils::sql_util::SqlUtil,
};

use crate::{extractor::base_extractor::BaseExtractor, Extractor};

pub struct MysqlSnapshotExtractor<'a> {
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub buffer: &'a ConcurrentQueue<DtData>,
    pub slice_size: usize,
    pub db: String,
    pub tb: String,
    pub shut_down: &'a AtomicBool,
}

#[async_trait]
impl Extractor for MysqlSnapshotExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "MysqlSnapshotExtractor starts, schema: {}, tb: {}, slice_size: {}",
            self.db,
            self.tb,
            self.slice_size
        );
        self.extract_internal().await
    }

    async fn close(&mut self) -> Result<(), Error> {
        if self.conn_pool.is_closed() {
            return Ok(());
        }
        return Ok(self.conn_pool.close().await);
    }
}

impl MysqlSnapshotExtractor<'_> {
    pub async fn extract_internal(&mut self) -> Result<(), Error> {
        let tb_meta = self.meta_manager.get_tb_meta(&self.db, &self.tb).await?;

        if let Some(order_col) = &tb_meta.basic.order_col {
            let order_col_type = tb_meta.col_type_map.get(order_col).unwrap();
            self.extract_by_slices(&tb_meta, order_col, order_col_type, ColValue::None)
                .await?;
        } else {
            self.extract_all(&tb_meta).await?;
        }

        BaseExtractor::wait_task_finish(self.buffer, self.shut_down).await
    }

    async fn extract_all(&mut self, tb_meta: &MysqlTbMeta) -> Result<(), Error> {
        log_info!(
            "start extracting data from {}.{} without slices",
            self.db,
            self.tb
        );

        let sql_util = SqlUtil::new_for_mysql(tb_meta);
        let mut all_count = 0;
        let sql = format!(
            "SELECT * FROM {}.{}",
            sql_util.quote(&self.db),
            sql_util.quote(&self.tb)
        );
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_mysql_row(&row, &tb_meta);
            BaseExtractor::push_row(self.buffer, row_data)
                .await
                .unwrap();
            all_count += 1;
        }

        log_info!(
            "end extracting data from {}.{}, all count: {}",
            self.db,
            self.tb,
            all_count
        );
        Ok(())
    }

    async fn extract_by_slices(
        &mut self,
        tb_meta: &MysqlTbMeta,
        order_col: &str,
        order_col_type: &MysqlColType,
        init_start_value: ColValue,
    ) -> Result<(), Error> {
        log_info!(
            "start extracting data from {}.{} by slices",
            self.db,
            self.tb
        );

        let sql_util = SqlUtil::new_for_mysql(tb_meta);
        let db = sql_util.quote(&self.db);
        let tb = sql_util.quote(&self.tb);
        let quoted_order_col = sql_util.quote(order_col);

        let mut all_count = 0;
        let mut start_value = init_start_value;
        let sql1 = format!(
            "SELECT * FROM {}.{} ORDER BY {} ASC LIMIT {}",
            db, tb, quoted_order_col, self.slice_size
        );
        let sql2 = format!(
            "SELECT * FROM {}.{} WHERE {} > ? ORDER BY {} ASC LIMIT {}",
            db, tb, quoted_order_col, quoted_order_col, self.slice_size
        );

        loop {
            let start_value_for_bind = start_value.clone();
            let query = if let ColValue::None = start_value {
                sqlx::query(&sql1)
            } else {
                sqlx::query(&sql2).bind_col_value(Some(&start_value_for_bind), &order_col_type)
            };

            let mut rows = query.fetch(&self.conn_pool);
            let mut slice_count = 0usize;
            while let Some(row) = rows.try_next().await.unwrap() {
                let row_data = RowData::from_mysql_row(&row, &tb_meta);
                BaseExtractor::push_row(self.buffer, row_data)
                    .await
                    .unwrap();
                start_value =
                    MysqlColValueConvertor::from_query(&row, order_col, order_col_type).unwrap();
                slice_count += 1;
                all_count += 1;
            }

            // all data extracted
            if slice_count < self.slice_size {
                break;
            }
        }

        log_info!(
            "end extracting data from {}.{}, all count: {}",
            self.db,
            self.tb,
            all_count
        );
        Ok(())
    }
}
