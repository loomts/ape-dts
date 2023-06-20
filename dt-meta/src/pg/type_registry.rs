use std::collections::HashMap;

use dt_common::error::Error;
use futures::TryStreamExt;
use sqlx::{postgres::PgRow, Pool, Postgres, Row};

use super::pg_col_type::PgColType;

#[derive(Clone)]
pub struct TypeRegistry {
    pub conn_pool: Pool<Postgres>,
    pub oid_to_type: HashMap<i32, PgColType>,
    pub name_to_type: HashMap<String, PgColType>,
}

impl TypeRegistry {
    pub fn new(conn_pool: Pool<Postgres>) -> Self {
        Self {
            conn_pool,
            oid_to_type: HashMap::new(),
            name_to_type: HashMap::new(),
        }
    }

    pub async fn init(mut self) -> Result<Self, Error> {
        // TODO check duplicate typename in pg_catalog.pg_type
        let sql = "SELECT t.oid AS oid,
                    t.typname AS name,
                    t.typelem AS element,
                    t.typbasetype AS parentoid,
                    t.typtypmod AS modifiers,
                    t.typcategory AS category,
                    e.values AS enum_values
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n
            ON (t.typnamespace = n.oid)
            LEFT JOIN 
            (SELECT t.enumtypid AS id, array_agg(t.enumlabel) AS values
            FROM pg_catalog.pg_enum t
            GROUP BY id) e
            ON (t.oid = e.id)
            WHERE n.nspname != 'pg_toast'";
        let mut rows = sqlx::query(sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let col_type = self.parse_col_meta(&row)?;
            self.oid_to_type.insert(col_type.oid, col_type.clone());
            self.name_to_type
                .insert(col_type.long_name.clone(), col_type.clone());
        }
        Ok(self)
    }

    fn parse_col_meta(&mut self, row: &PgRow) -> Result<PgColType, Error> {
        let oid: i32 = row.get_unchecked("oid");
        // cast to short name
        let long_name: String = row.try_get("name")?;
        let short_name = PgColType::get_short_name(&long_name);
        let element_oid: i32 = row.get_unchecked("element");
        let parent_oid: i32 = row.get_unchecked("parentoid");
        let modifiers: i32 = row.get_unchecked("modifiers");
        let category: String = row.get_unchecked("category");
        let enum_values: Option<Vec<u8>> = row.get_unchecked("enum_values");
        let enum_values = if enum_values.is_none() {
            None
        } else {
            let enum_values: Vec<String> = row.try_get("enum_values")?;
            Some(enum_values)
        };

        Ok(PgColType {
            oid,
            long_name,
            short_name,
            element_oid,
            parent_oid,
            modifiers,
            category,
            enum_values,
        })
    }
}
