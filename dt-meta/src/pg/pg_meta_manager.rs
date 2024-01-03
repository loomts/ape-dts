use std::collections::HashMap;

use dt_common::error::Error;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres, Row};

use crate::{
    foreign_key::ForeignKey, rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta,
    row_data::RowData,
};

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

    pub async fn get_tb_meta_by_row_data<'a>(
        &'a mut self,
        row_data: &RowData,
    ) -> Result<&'a PgTbMeta, Error> {
        self.get_tb_meta(&row_data.schema, &row_data.tb).await
    }

    pub async fn get_tb_meta<'a>(
        &'a mut self,
        schema: &str,
        tb: &str,
    ) -> Result<&'a PgTbMeta, Error> {
        let full_name = format!(r#""{}"."{}""#, schema, tb);
        if !self.name_to_tb_meta.contains_key(&full_name) {
            let oid = Self::get_oid(&self.conn_pool, schema, tb).await?;
            let (cols, col_type_map) =
                Self::parse_cols(&self.conn_pool, &mut self.type_registry, schema, tb).await?;
            let key_map = Self::parse_keys(&self.conn_pool, schema, tb).await?;
            let (order_col, partition_col, id_cols) =
                RdbMetaManager::parse_rdb_cols(&key_map, &cols)?;
            let foreign_keys = Self::get_foreign_keys(&self.conn_pool, schema, tb).await?;

            let basic = RdbTbMeta {
                schema: schema.to_string(),
                tb: tb.to_string(),
                cols,
                key_map,
                order_col,
                partition_col,
                id_cols,
                foreign_keys,
            };
            let tb_meta = PgTbMeta {
                oid,
                col_type_map,
                basic,
            };
            self.oid_to_tb_meta.insert(oid, tb_meta.clone());
            self.name_to_tb_meta.insert(full_name.clone(), tb_meta);
        }
        Ok(self.name_to_tb_meta.get(&full_name).unwrap())
    }

    pub fn invalidate_cache(&mut self, schema: &str, tb: &str) {
        // TODO, if schema is not empty but tb is empty, only clear cache for the schema
        if !schema.is_empty() && !tb.is_empty() {
            let full_name = format!(r#""{}"."{}""#, schema, tb);
            self.name_to_tb_meta.remove(&full_name);
        } else {
            self.name_to_tb_meta.clear();
        }
    }

    async fn parse_cols(
        conn_pool: &Pool<Postgres>,
        type_registry: &mut TypeRegistry,
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
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
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

        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let col: String = row.try_get("col_name")?;
            if !cols.contains(&col) {
                continue;
            }

            let col_type_oid: i32 = row.try_get_unchecked("col_type_oid")?;
            let col_type = type_registry
                .oid_to_type
                .get(&col_type_oid)
                .unwrap()
                .clone();
            col_type_map.insert(col, col_type);
        }

        Ok((cols, col_type_map))
    }

    async fn parse_keys(
        conn_pool: &Pool<Postgres>,
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
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
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

    async fn get_oid(conn_pool: &Pool<Postgres>, schema: &str, tb: &str) -> Result<i32, Error> {
        let sql = format!(r#"SELECT '"{}"."{}"'::regclass::oid;"#, schema, tb);
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        if let Some(row) = rows.try_next().await.unwrap() {
            let oid: i32 = row.try_get_unchecked("oid")?;
            return Ok(oid);
        }

        Err(Error::MetadataError(format!(
            "failed to get oid for: {} by query: {}",
            tb, sql
        )))
    }

    async fn get_foreign_keys(
        conn_pool: &Pool<Postgres>,
        schema: &str,
        tb: &str,
    ) -> Result<Vec<ForeignKey>, Error> {
        let mut foreign_keys = Vec::new();
        let sql = format!(
            "SELECT
            a1.attname AS column_name,
            ns_ref.nspname AS referenced_schema_name,
            tab_ref.relname AS referenced_table_name,
            a2.attname AS referenced_column_name
        FROM
            pg_constraint c
            INNER JOIN pg_class tab ON tab.oid = c.conrelid
            INNER JOIN pg_namespace ns ON ns.oid = tab.relnamespace
            INNER JOIN pg_attribute a1 ON a1.attnum = ANY(c.conkey) AND a1.attrelid = c.conrelid
            INNER JOIN pg_class tab_ref ON tab_ref.oid = c.confrelid
            INNER JOIN pg_namespace ns_ref ON ns_ref.oid = tab_ref.relnamespace
            INNER JOIN pg_attribute a2 ON a2.attnum = ANY(c.confkey) AND a2.attrelid = c.confrelid
        WHERE
            c.contype = 'f' 
            AND ns.nspname = '{}' 
            AND tab.relname = '{}'",
            schema, tb
        );

        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let col: String = row.try_get("column_name")?;
            let ref_schema: String = row.try_get("referenced_schema_name")?;
            let ref_tb: String = row.try_get("referenced_table_name")?;
            let ref_col: String = row.try_get("referenced_column_name")?;
            foreign_keys.push(ForeignKey {
                col: col.to_lowercase(),
                ref_schema: ref_schema.to_lowercase(),
                ref_tb: ref_tb.to_lowercase(),
                ref_col: ref_col.to_lowercase(),
            });
        }
        Ok(foreign_keys)
    }
}
