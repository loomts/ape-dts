use std::sync::atomic::AtomicBool;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use futures::TryStreamExt;

use sqlx::{Pool, Postgres};

use dt_common::{
    log_info,
    meta::{
        col_value::ColValue,
        dt_data::DtData,
        pg::{pg_col_type::PgColType, pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        row_data::RowData,
    },
    utils::sql_util::SqlUtil,
};

use dt_common::{
    adaptor::{pg_col_value_convertor::PgColValueConvertor, sqlx_ext::SqlxPgExt},
    error::Error,
};

use crate::{extractor::base_extractor::BaseExtractor, Extractor};

pub struct PgSnapshotExtractor<'a> {
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub buffer: &'a ConcurrentQueue<DtData>,
    pub slice_size: usize,
    pub schema: String,
    pub tb: String,
    pub shut_down: &'a AtomicBool,
}

#[async_trait]
impl Extractor for PgSnapshotExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "PgSnapshotExtractor starts, schema: {}, tb: {}, slice_size: {}",
            self.schema,
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

impl PgSnapshotExtractor<'_> {
    pub async fn extract_internal(&mut self) -> Result<(), Error> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&self.schema, &self.tb)
            .await?;

        if let Some(order_col) = &tb_meta.basic.order_col {
            let order_col_type = tb_meta.col_type_map.get(order_col);
            self.extract_by_slices(
                &tb_meta,
                &order_col,
                order_col_type.unwrap(),
                ColValue::None,
            )
            .await?;
        } else {
            self.extract_all(&tb_meta).await?;
        }

        BaseExtractor::wait_task_finish(self.buffer, self.shut_down).await
    }

    async fn extract_all(&mut self, tb_meta: &PgTbMeta) -> Result<(), Error> {
        log_info!(
            "start extracting data from {}.{} without slices",
            self.schema,
            self.tb
        );

        let mut all_count = 0;
        let sql = self.build_extract_sql(tb_meta, false);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_pg_row(&row, &tb_meta);
            BaseExtractor::push_row(self.buffer, row_data)
                .await
                .unwrap();
            all_count += 1;
        }

        log_info!(
            "end extracting data from {}.{}, all count: {}",
            self.schema,
            self.tb,
            all_count
        );
        Ok(())
    }

    async fn extract_by_slices(
        &mut self,
        tb_meta: &PgTbMeta,
        order_col: &str,
        order_col_type: &PgColType,
        init_start_value: ColValue,
    ) -> Result<(), Error> {
        log_info!(
            "start extracting data from {}.{} by slices",
            self.schema,
            self.tb
        );

        let mut all_count = 0;
        let mut start_value = init_start_value;
        let sql1 = self.build_extract_sql(tb_meta, false);
        let sql2 = self.build_extract_sql(tb_meta, true);
        loop {
            let start_value_for_bind = start_value.clone();
            let query = if let ColValue::None = start_value {
                sqlx::query(&sql1)
            } else {
                sqlx::query(&sql2).bind_col_value(Some(&start_value_for_bind), order_col_type)
            };

            let mut rows = query.fetch(&self.conn_pool);
            let mut slice_count = 0usize;
            while let Some(row) = rows.try_next().await.unwrap() {
                let row_data = RowData::from_pg_row(&row, &tb_meta);
                BaseExtractor::push_row(self.buffer, row_data)
                    .await
                    .unwrap();

                start_value =
                    PgColValueConvertor::from_query(&row, order_col, order_col_type).unwrap();
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
            self.schema,
            self.tb,
            all_count
        );
        Ok(())
    }

    fn build_extract_sql(&mut self, tb_meta: &PgTbMeta, has_start_value: bool) -> String {
        let sql_util = SqlUtil::new_for_pg(tb_meta);
        let cols_str = sql_util.build_extract_cols_str().unwrap();
        let schema = sql_util.quote(&self.schema);
        let tb = sql_util.quote(&self.tb);

        // SELECT col_1, col_2::text FROM tb_1 WHERE col_1 > $1 ORDER BY col_1;
        if let Some(order_col) = &tb_meta.basic.order_col {
            let quoted_order_col = sql_util.quote(&order_col);
            if has_start_value {
                let order_col_type = tb_meta.col_type_map.get(order_col).unwrap();
                return format!(
                    "SELECT {} FROM {}.{} WHERE {} > $1::{} ORDER BY {} ASC LIMIT {}",
                    cols_str,
                    schema,
                    tb,
                    quoted_order_col,
                    order_col_type.short_name,
                    quoted_order_col,
                    self.slice_size
                );
            } else {
                return format!(
                    "SELECT {} FROM {}.{} ORDER BY {} ASC LIMIT {}",
                    cols_str, schema, tb, quoted_order_col, self.slice_size
                );
            }
        } else {
            return format!("SELECT {} FROM {}.{}", cols_str, schema, tb);
        }
    }
}
