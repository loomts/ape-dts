use std::collections::HashMap;

use futures::TryStreamExt;
use sqlx::{postgres::PgRow, Pool, Postgres, Row};

use crate::error::Error;

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
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await? {
            let typee = self.parse_col_meta(&row)?;
            self.oid_to_type.insert(typee.oid.clone(), typee.clone());
            self.name_to_type.insert(typee.name.clone(), typee.clone());
        }
        Ok(self)
    }

    fn parse_col_meta(&mut self, row: &PgRow) -> Result<PgColType, Error> {
        let oid: i32 = row.get_unchecked("oid");
        let name: String = row.try_get("name")?;
        let element_oid: i32 = row.get_unchecked("element");
        let parent_oid: i32 = row.get_unchecked("parentoid");
        let modifiers: i32 = row.get_unchecked("modifiers");
        let category: String = row.get_unchecked("category");
        let enum_values: Option<Vec<u8>> = row.get_unchecked("enum_values");
        let enum_values = if None == enum_values {
            "".to_string()
        } else {
            row.try_get("enum_values")?
        };

        Ok(PgColType {
            oid,
            name,
            element_oid,
            parent_oid,
            modifiers,
            category,
            enum_values,
        })
    }
}
