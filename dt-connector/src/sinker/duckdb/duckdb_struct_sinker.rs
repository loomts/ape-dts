use crate::{rdb_router::RdbRouter, Sinker};

use anyhow::bail;
use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    error::Error,
    log_error, log_info,
    meta::{
        mysql::{mysql_col_type::MysqlColType, mysql_tb_meta::MysqlTbMeta},
        pg::pg_tb_meta::PgTbMeta,
        rdb_meta_manager::RdbMetaManager,
        struct_meta::{
            statement::struct_statement::StructStatement, struct_data::StructData,
            structure::table::Table,
        },
    },
    rdb_filter::RdbFilter,
};

use async_trait::async_trait;
use duckdb::Connection;

pub struct DuckdbStructSinker {
    pub conn: Option<Connection>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub router: RdbRouter,
    pub extractor_meta_manager: RdbMetaManager,
}

#[async_trait]
impl Sinker for DuckdbStructSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        for i in data {
            match i.statement {
                StructStatement::MysqlCreateDatabase(statement) => {
                    let sql = format!(
                        r#"CREATE SCHEMA IF NOT EXISTS "{}""#,
                        statement.database.name
                    );
                    self.execute_sql(&sql)?;
                }

                StructStatement::MysqlCreateTable(statement) => {
                    let schema = &statement.table.database_name;
                    let tb = &statement.table.table_name;
                    if let Some(meta_manager) =
                        self.extractor_meta_manager.mysql_meta_manager.as_mut()
                    {
                        let tb_meta = meta_manager.get_tb_meta(schema, tb).await?;
                        let sql = Self::get_create_table_sql_from_mysql(&statement.table, tb_meta)?;
                        self.execute_sql(&sql)?;
                    }
                }

                StructStatement::PgCreateSchema(statement) => {
                    let sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, statement.schema.name);
                    self.execute_sql(&sql)?;
                }

                StructStatement::PgCreateTable(statement) => {
                    let schema = &statement.table.schema_name;
                    let tb = &statement.table.table_name;
                    if let Some(meta_manager) = self.extractor_meta_manager.pg_meta_manager.as_mut()
                    {
                        let tb_meta = meta_manager.get_tb_meta(schema, tb).await?;
                        let sql = Self::get_create_table_sql_from_pg(&statement.table, tb_meta)?;
                        self.execute_sql(&sql)?;
                    }
                }

                _ => {}
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(conn) = self.conn.take() {
            if let Err((_, err)) = conn.close() {
                bail!(Error::DuckdbError(err))
            }
        }
        Ok(())
    }
}

impl DuckdbStructSinker {
    fn get_create_table_sql_from_mysql(
        table: &Table,
        tb_meta: &MysqlTbMeta,
    ) -> anyhow::Result<String> {
        let mut dst_cols = vec![];
        for column in table.columns.iter() {
            let col = &column.column_name;
            let mysql_col_type = tb_meta.get_col_type(col)?;
            let dst_col_type = match mysql_col_type {
                MysqlColType::TinyInt { unsigned: false } => "TINYINT",
                MysqlColType::TinyInt { unsigned: true } => "UTINYINT",
                MysqlColType::SmallInt { unsigned: false } => "SMALLINT",
                MysqlColType::SmallInt { unsigned: true } => "USMALLINT",
                MysqlColType::MediumInt { unsigned: false } => "INTEGER",
                MysqlColType::MediumInt { unsigned: true } => "UINTEGER",
                MysqlColType::Int { unsigned: false } => "INTEGER",
                MysqlColType::Int { unsigned: true } => "UINTEGER",
                MysqlColType::BigInt { unsigned: false } => "BIGINT",
                MysqlColType::BigInt { unsigned: true } => "UBIGINT",

                MysqlColType::Float => "FLOAT",
                MysqlColType::Double => "DOUBLE",
                MysqlColType::Decimal { precision, scale } => {
                    format!("DECIMAL({},{})", precision, scale).leak()
                }

                MysqlColType::Time => "INTERVAL",
                MysqlColType::Date => "DATE",
                MysqlColType::DateTime => "DATETIME",
                MysqlColType::Timestamp { timezone_offset: _ } => "TIMESTAMP",
                MysqlColType::Year => "SMALLINT",

                MysqlColType::Char { .. }
                | MysqlColType::Varchar { .. }
                | MysqlColType::TinyText { .. }
                | MysqlColType::MediumText { .. }
                | MysqlColType::Text { .. }
                | MysqlColType::LongText { .. } => "VARCHAR",

                MysqlColType::Binary { length: _ }
                | MysqlColType::VarBinary { length: _ }
                | MysqlColType::TinyBlob
                | MysqlColType::MediumBlob
                | MysqlColType::Blob
                | MysqlColType::LongBlob => "BLOB",

                MysqlColType::Bit => "UBIGINT",
                MysqlColType::Set { items: _ } => "VARCHAR",
                MysqlColType::Enum { items } => {
                    let escaped_items: Vec<String> =
                        items.iter().map(|i| format!("'{}'", i)).collect();
                    format!("ENUM({})", escaped_items.join(",")).leak()
                }
                MysqlColType::Json => "JSON",
                MysqlColType::Unknown => "VARCHAR",
            };

            if column.is_nullable {
                dst_cols.push(format!(r#""{}" {}"#, col, dst_col_type));
            } else {
                dst_cols.push(format!(r#""{}" {} NOT NULL"#, col, dst_col_type));
            }
        }

        let mut sql = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"."{}" ({}"#,
            table.database_name,
            table.table_name,
            dst_cols.join(", "),
        );

        // primary key
        if let Some(pks) = tb_meta.basic.key_map.get("primary") {
            let escaped_pks: Vec<String> = pks.iter().map(|i| format!(r#""{}""#, i)).collect();
            sql = format!("{}, PRIMARY KEY({}));", sql, escaped_pks.join(","))
        } else {
            sql = format!("{});", sql)
        }

        Ok(sql)
    }

    fn get_create_table_sql_from_pg(table: &Table, tb_meta: &PgTbMeta) -> anyhow::Result<String> {
        let mut dst_cols = vec![];
        for column in table.columns.iter() {
            let col = &column.column_name;
            let pg_col_type = tb_meta.get_col_type(col)?;
            let dst_col_type = match pg_col_type.alias.as_str() {
                "bpchar" | "varchar" | "text" => "VARCHAR",
                "float4" => "FLOAT",
                "float8" => "DOUBLE",
                "decimal" => "DECIMAL",
                "int2" => "SMALLINT",
                "int4" => "INTEGER",
                "int8" => "BIGINT",
                "bit" | "varbit" => "BIT",
                "time" => "TIME",
                "timetz" => "TIMETZ",
                "timestamp" => "TIMESTAMP",
                "timestamptz" => "TIMESTAMPTZ",
                "bool" => "BOOLEAN",
                "bytea" => "BLOB",
                "interval" => "INTERVAL",
                "json" | "jsonb" => "JSON",
                "uuid" => "UUID",
                _ => "VARCHAR",
            };

            if column.is_nullable {
                dst_cols.push(format!(r#""{}" {}"#, col, dst_col_type));
            } else {
                dst_cols.push(format!(r#""{}" {} NOT NULL"#, col, dst_col_type));
            }
        }

        let mut sql = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"."{}" ({}"#,
            table.database_name,
            table.table_name,
            dst_cols.join(", "),
        );

        // primary key
        if let Some(pks) = tb_meta.basic.key_map.get("primary") {
            let escaped_pks: Vec<String> = pks.iter().map(|i| format!(r#""{}""#, i)).collect();
            sql = format!("{}, PRIMARY KEY({}));", sql, escaped_pks.join(","))
        } else {
            sql = format!("{});", sql)
        }

        Ok(sql)
    }

    fn execute_sql(&self, sql: &str) -> anyhow::Result<()> {
        log_info!("ddl begin: {}", sql);
        match self.conn.as_ref().unwrap().execute(sql, []) {
            Ok(_) => {
                log_info!("ddl succeed");
            }

            Err(error) => {
                log_error!("ddl failed, error: {}", error);
                match self.conflict_policy {
                    ConflictPolicyEnum::Interrupt => bail! {error},
                    ConflictPolicyEnum::Ignore => {}
                }
            }
        }
        Ok(())
    }
}
