use futures::executor::block_on;
use sqlx::{MySql, Pool};

use crate::{
    error::Error,
    meta::{
        mysql::{mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta},
        row_data::RowData,
    },
    traits::{
        sqlx_ext::SqlxMysql,
        traits::{Sinker, Sinker2},
    },
};

use super::{rdb_router::RdbRouter, rdb_sinker_util::RdbSinkerUtil};

use async_trait::async_trait;

#[derive(Clone)]
pub struct MysqlSinker {
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub router: RdbRouter,
    pub batch_size: usize,
}

#[async_trait]
impl Sinker for MysqlSinker {
    async fn sink(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }

        // currently only support batch insert
        if self.batch_size > 1 {
            self.batch_insert(data).await
        } else {
            self.sink_internal(data).await
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        if self.conn_pool.is_closed() {
            return Ok(());
        }
        return Ok(self.conn_pool.close().await);
    }
}

// impl Sinker2 for MysqlSinker {
//     fn sink(&mut self, data: Vec<RowData>) -> Result<(), Error> {
//         if data.len() == 0 {
//             return Ok(());
//         }

//         // currently only support batch insert
//         if self.batch_size > 1 {
//             block_on(self.batch_insert(data))
//         } else {
//             block_on(self.sink_internal(data))
//         }
//     }

//     fn close(&mut self) -> Result<(), Error> {
//         if self.conn_pool.is_closed() {
//             return Ok(());
//         }
//         Ok(block_on(self.conn_pool.close()))
//     }
// }

impl MysqlSinker {
    async fn sink_internal(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        for row_data in data.iter() {
            let tb_meta = self.get_tb_meta(&row_data).await?;
            let rdb_util = RdbSinkerUtil::new_for_mysql(tb_meta);

            let (sql, _cols, binds) = rdb_util.get_query(&row_data)?;
            let mut query = sqlx::query(&sql);
            for bind in binds {
                query = query.bind_col_value(bind);
            }

            let result = query.execute(&self.conn_pool).await.unwrap();
            rdb_util.check_result(result.rows_affected(), 1, &sql, row_data)?;
        }
        Ok(())
    }

    async fn batch_insert(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        let all_count = data.len();
        let mut sinked_count = 0;

        let first_row_data = &data[0];
        let tb_meta = self.get_tb_meta(first_row_data).await?;
        let rdb_util = RdbSinkerUtil::new_for_mysql(tb_meta);

        loop {
            let mut batch_size = self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            let (sql, _cols, binds) =
                rdb_util.get_batch_insert_query(&data, sinked_count, batch_size)?;
            let mut query = sqlx::query(&sql);
            for bind in binds {
                query = query.bind_col_value(bind);
            }

            let result = query.execute(&self.conn_pool).await.unwrap();
            rdb_util.check_result(
                result.rows_affected(),
                batch_size as u64,
                &sql,
                first_row_data,
            )?;

            sinked_count += batch_size;
            if sinked_count == all_count {
                break;
            }
        }

        Ok(())
    }

    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<MysqlTbMeta, Error> {
        let (db, tb) = self.router.get_route(&row_data.db, &row_data.tb);
        let tb_meta = self.meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }
}
