use super::{
    mysql_dbengine_meta_center::MysqlDbEngineMetaCenter, mysql_meta_fetcher::MysqlMetaFetcher,
    mysql_tb_meta::MysqlTbMeta,
};
use crate::meta::row_data::RowData;
use crate::{config::config_enums::DbType, meta::ddl_meta::ddl_data::DdlData};
use sqlx::{MySql, Pool};

#[derive(Clone)]
pub struct MysqlMetaManager {
    pub meta_center: Option<MysqlDbEngineMetaCenter>,
    pub meta_fetcher: MysqlMetaFetcher,
}

impl MysqlMetaManager {
    pub async fn new(conn_pool: Pool<MySql>) -> anyhow::Result<Self> {
        Self::new_mysql_compatible(conn_pool, DbType::Mysql).await
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        if let Some(meta_center) = &self.meta_center {
            meta_center.meta_fetcher.close().await?;
        }
        self.meta_fetcher.close().await
    }

    pub async fn new_mysql_compatible(
        conn_pool: Pool<MySql>,
        db_type: DbType,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            meta_center: None,
            meta_fetcher: MysqlMetaFetcher::new_mysql_compatible(conn_pool, db_type).await?,
        })
    }

    pub fn invalidate_cache(&mut self, schema: &str, tb: &str) {
        if let Some(meta_center) = &mut self.meta_center {
            meta_center.meta_fetcher.invalidate_cache(schema, tb);
        }
        self.meta_fetcher.invalidate_cache(schema, tb)
    }

    pub fn invalidate_cache_by_ddl_data(&mut self, ddl_data: &DdlData) {
        let (schema, tb) = ddl_data.get_schema_tb();
        self.invalidate_cache(&schema, &tb);
    }

    pub async fn get_tb_meta_by_row_data<'a>(
        &'a mut self,
        row_data: &RowData,
    ) -> anyhow::Result<&'a MysqlTbMeta> {
        self.get_tb_meta(&row_data.schema, &row_data.tb).await
    }

    pub async fn get_tb_meta<'a>(
        &'a mut self,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<&'a MysqlTbMeta> {
        if let Some(meta_center) = &mut self.meta_center {
            if let Ok(tb_meta) = meta_center.meta_fetcher.get_tb_meta(schema, tb).await {
                return Ok(tb_meta);
            }
        }
        self.meta_fetcher.get_tb_meta(schema, tb).await
    }
}
