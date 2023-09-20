use std::collections::HashMap;

use dt_common::error::Error;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres, Row};

use crate::{rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta};

use super::{pg_col_type::PgColType, pg_tb_meta::PgTbMeta, type_registry::TypeRegistry};

#[derive(Clone)]
pub struct PgMetaManager {
    pub conn_pool: Pool<Postgres>,
    pub type_registry: TypeRegistry,
    pub name_to_tb_meta: HashMap<String, PgTbMeta>,
    pub oid_to_tb_meta: HashMap<i32, PgTbMeta>,
}

impl PgMetaManager {
    pub fn new(conn_pool: Pool<Postgres>) -> Self {
        let type_registry = TypeRegistry::new(conn_pool.clone());
        PgMetaManager {
            conn_pool,
            type_registry,
            name_to_tb_meta: HashMap::new(),
            oid_to_tb_meta: HashMap::new(),
        }
    }

    pub async fn init(mut self) -> Result<Self, Error> {
        self.type_registry = self.type_registry.init().await?;
        Ok(self)
    }

    pub fn get_col_type_by_oid(&mut self, oid: i32) -> Result<PgColType, Error> {
        Ok(self.type_registry.oid_to_type.get(&oid).unwrap().clone())
    }

    pub fn update_tb_meta_by_oid(&mut self, oid: i32, tb_meta: PgTbMeta) -> Result<(), Error> {
        self.oid_to_tb_meta.insert(oid, tb_meta.clone());
        let full_name = format!(r#""{}"."{}""#, &tb_meta.basic.schema, &tb_meta.basic.tb);
        self.name_to_tb_meta.insert(full_name, tb_meta);
        Ok(())
    }

    pub fn get_tb_meta_by_oid(&mut self, oid: i32) -> Result<PgTbMeta, Error> {
        Ok(self.oid_to_tb_meta.get(&oid).unwrap().clone())
    }

    pub async fn get_tb_meta(&mut self, schema: &str, tb: &str) -> Result<PgTbMeta, Error> {
        let full_name = format!(r#""{}"."{}""#, schema, tb);
        if let Some(tb_meta) = self.name_to_tb_meta.get(&full_name) {
            return Ok(tb_meta.clone());
        }

        let oid = self.get_oid(schema, tb).await?;
        let (cols, col_type_map) = self.parse_cols(schema, tb).await?;
        let key_map = self.parse_keys(schema, tb).await?;
        let (order_col, partition_col, id_cols) = RdbMetaManager::parse_rdb_cols(&key_map, &cols)?;

        let basic = RdbTbMeta {
            schema: schema.to_string(),
            tb: tb.to_string(),
            cols,
            key_map,
            order_col,
            partition_col,
            id_cols,
        };
        let tb_meta = PgTbMeta {
            oid,
            col_type_map,
            basic,
        };

        self.name_to_tb_meta.insert(full_name, tb_meta.clone());
        self.oid_to_tb_meta.insert(oid, tb_meta.clone());
        Ok(tb_meta)
    }

    async fn parse_cols(
        &mut self,
        schema: &str,
        tb: &str,
    ) -> Result<(Vec<String>, HashMap<String, PgColType>), Error> {
        let mut cols = Vec::new();
        let mut col_type_map = HashMap::new();

        // get cols of the table
        let sql = format!(
            "SELECT column_name FROM information_schema.columns 
            WHERE table_schema='{}' AND table_name = '{}' 
            ORDER BY ordinal_position;",
            schema, tb
        );
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let col: String = row.try_get("column_name")?;
            cols.push(col);
        }

        // get col_type_oid of the table
        let sql = format!(
            "SELECT a.attname AS col_name, a.atttypid as col_type_oid
            FROM pg_class t, pg_attribute a
            WHERE a.attrelid = t.oid
                AND t.relname = '{}'
                AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{}');",
            tb, schema
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let col: String = row.try_get("col_name")?;
            if !cols.contains(&col) {
                continue;
            }

            let col_type_oid: i32 = row.try_get_unchecked("col_type_oid")?;
            let col_type = self
                .type_registry
                .oid_to_type
                .get(&col_type_oid)
                .unwrap()
                .clone();
            col_type_map.insert(col, col_type);
        }

        Ok((cols, col_type_map))
    }

    async fn parse_keys(
        &self,
        schema: &str,
        tb: &str,
    ) -> Result<HashMap<String, Vec<String>>, Error> {
        // TODO, find pk and use pk as where_cols if tables has pk
        let sql = format!(
            "SELECT t.relname AS tb_name,
            i.relname AS index_name,
            a.attname AS col_name,
            ix.indisprimary AS is_primary
            FROM pg_class t, pg_class i, pg_index ix, pg_attribute a
            WHERE t.oid = ix.indrelid
                AND i.oid = ix.indexrelid
                AND a.attrelid = t.oid
                AND a.attnum = ANY(ix.indkey)
                AND t.relkind = 'r'
                AND t.relname = '{}'
                AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{}')
                AND ix.indisunique = true
            ORDER BY  t.relname, i.relname;",
            tb, schema
        );

        let mut key_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let mut col_name: String = row.try_get("col_name")?;
            col_name = col_name.to_lowercase();

            let is_primary: bool = row.try_get("is_primary")?;
            let mut key_name: String = row.try_get("index_name")?;
            key_name = if is_primary {
                "primary".to_string()
            } else {
                key_name.to_lowercase()
            };

            // key_map
            if let Some(key_cols) = key_map.get_mut(&key_name) {
                key_cols.push(col_name);
            } else {
                key_map.insert(key_name, vec![col_name]);
            }
        }
        Ok(key_map)
    }

    async fn get_oid(&self, schema: &str, tb: &str) -> Result<i32, Error> {
        let sql = format!(r#"SELECT '"{}"."{}"'::regclass::oid;"#, schema, tb);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await.unwrap() {
            let oid: i32 = row.try_get_unchecked("oid")?;
            return Ok(oid);
        }

        Err(Error::MetadataError(format!(
            "failed to get oid for: {} by query: {}",
            tb, sql
        )))
    }
}
