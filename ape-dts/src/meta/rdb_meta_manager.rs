use crate::error::Error;

use super::{
    mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager,
    rdb_tb_meta::RdbTbMeta,
};

pub struct RdbMetaManager {
    mysql_meta_manager: Option<MysqlMetaManager>,
    pg_meta_manager: Option<PgMetaManager>,
}

impl RdbMetaManager {
    pub fn from_mysql(mysql_meta_manager: MysqlMetaManager) -> Self {
        Self {
            mysql_meta_manager: Some(mysql_meta_manager),
            pg_meta_manager: Option::None,
        }
    }

    pub fn from_pg(pg_meta_manager: PgMetaManager) -> Self {
        Self {
            mysql_meta_manager: Option::None,
            pg_meta_manager: Some(pg_meta_manager),
        }
    }

    pub async fn get_tb_meta(&mut self, schema: &str, tb: &str) -> Result<RdbTbMeta, Error> {
        if let Some(mysql_meta_manager) = self.mysql_meta_manager.as_mut() {
            let tb_meta = mysql_meta_manager.get_tb_meta(schema, tb).await?;
            return Ok(tb_meta.basic);
        }

        if let Some(pg_meta_manager) = self.pg_meta_manager.as_mut() {
            let tb_meta = pg_meta_manager.get_tb_meta(schema, tb).await?;
            return Ok(tb_meta.basic);
        }

        Err(Error::Unexpected {
            error: "no available meta_manager in partitioner".to_string(),
        })
    }
}
