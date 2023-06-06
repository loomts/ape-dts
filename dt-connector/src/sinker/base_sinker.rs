use dt_common::error::Error;

use dt_meta::{
    mysql::{mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta},
    pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
    row_data::RowData,
};

use super::rdb_router::RdbRouter;

pub struct BaseSinker {}

#[macro_export(local_inner_macros)]
macro_rules! call_batch_fn {
    ($self:ident, $data:ident, $batch_fn:expr) => {
        let all_count = $data.len();
        let mut sinked_count = 0;

        loop {
            let mut batch_size = $self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            $batch_fn($self, &mut $data, sinked_count, batch_size)
                .await
                .unwrap();

            sinked_count += batch_size;
            if sinked_count == all_count {
                break;
            }
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! close_conn_pool {
    ($self:ident) => {
        if $self.conn_pool.is_closed() {
            Ok(())
        } else {
            Ok($self.conn_pool.close().await)
        }
    };
}

impl BaseSinker {
    #[inline(always)]
    pub async fn get_mysql_tb_meta(
        meta_manager: &mut MysqlMetaManager,
        router: &mut RdbRouter,
        row_data: &RowData,
    ) -> Result<MysqlTbMeta, Error> {
        let (db, tb) = router.get_route(&row_data.schema, &row_data.tb);
        let tb_meta = meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }

    #[inline(always)]
    pub async fn get_pg_tb_meta(
        meta_manager: &mut PgMetaManager,
        router: &mut RdbRouter,
        row_data: &RowData,
    ) -> Result<PgTbMeta, Error> {
        let (db, tb) = router.get_route(&row_data.schema, &row_data.tb);
        let tb_meta = meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }
}
