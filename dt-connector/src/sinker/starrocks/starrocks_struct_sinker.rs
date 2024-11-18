use crate::{rdb_router::RdbRouter, Sinker};

use anyhow::bail;
use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    log_error, log_info,
    meta::{
        mysql::{mysql_col_type::MysqlColType, mysql_tb_meta::MysqlTbMeta},
        rdb_meta_manager::RdbMetaManager,
        struct_meta::{
            statement::struct_statement::StructStatement, struct_data::StructData,
            structure::table::Table,
        },
    },
    rdb_filter::RdbFilter,
};

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};

const SIGN_COL_NAME: &str = "_ape_dts_is_deleted";
const SIGN_COL_TYPE: &str = "SMALLINT";
const VERSION_COL_NAME: &str = "_ape_dts_version";
const VERSION_COL_TYPE: &str = "BIGINT";

#[derive(Clone)]
pub struct StarrocksStructSinker {
    pub conn_pool: Pool<MySql>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub router: RdbRouter,
    pub extractor_meta_manager: RdbMetaManager,
    pub backend_count: i32,
}

#[async_trait]
impl Sinker for StarrocksStructSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        if self.backend_count == 0 {
            self.backend_count = self.get_backend_count().await?;
        }

        for i in data {
            match i.statement {
                StructStatement::MysqlCreateDatabase(statement) => {
                    let sql = format!(
                        "CREATE DATABASE IF NOT EXISTS `{}`",
                        statement.database.name
                    );
                    self.execute_sql(&sql).await?;
                }

                StructStatement::MysqlCreateTable(statement) => {
                    let schema = &statement.table.database_name;
                    let tb = &statement.table.table_name;
                    if let Some(meta_manager) =
                        self.extractor_meta_manager.mysql_meta_manager.as_mut()
                    {
                        let tb_meta = meta_manager.get_tb_meta(schema, tb).await?;
                        let sql = Self::get_create_table_sql_from_mysql(
                            &statement.table,
                            tb_meta,
                            self.backend_count,
                        )?;
                        self.execute_sql(&sql).await?;
                    }
                }

                _ => {}
            }
        }

        Ok(())
    }
}

impl StarrocksStructSinker {
    fn get_create_table_sql_from_mysql(
        table: &Table,
        tb_meta: &MysqlTbMeta,
        backend_count: i32,
    ) -> anyhow::Result<String> {
        let mut ck_cols = vec![];
        for column in table.columns.iter() {
            let col = &column.column_name;
            let mysql_col_type = tb_meta.get_col_type(col)?;
            let ck_col_type = match mysql_col_type {
                MysqlColType::TinyInt { unsigned: false } => "TINYINT",
                MysqlColType::TinyInt { unsigned: true } => "SMALLINT",
                MysqlColType::SmallInt { unsigned: false } => "SMALLINT",
                MysqlColType::SmallInt { unsigned: true } => "INT",
                MysqlColType::MediumInt { unsigned: false } => "INT",
                MysqlColType::MediumInt { unsigned: true } => "BIGINT",
                MysqlColType::Int { unsigned: false } => "INT",
                MysqlColType::Int { unsigned: true } => "BIGINT",
                MysqlColType::BigInt { unsigned: false } => "BIGINT",
                MysqlColType::BigInt { unsigned: true } => "LARGEINT",

                MysqlColType::Float => "FLOAT",
                MysqlColType::Double => "DOUBLE",
                MysqlColType::Decimal { precision, scale } => {
                    format!("DECIMAL({},{})", precision, scale).leak()
                }

                MysqlColType::Time => "VARCHAR",
                MysqlColType::Date => "DATE",
                MysqlColType::DateTime => "DATETIME",
                MysqlColType::Timestamp { timezone_offset: _ } => "VARCHAR",
                MysqlColType::Year => "INT",

                MysqlColType::Char { length: v, .. } => format!("CHAR({})", v).leak(),
                MysqlColType::Varchar { length: v, .. } => format!("VARCHAR({})", v).leak(),

                MysqlColType::TinyText { .. }
                | MysqlColType::MediumText { .. }
                | MysqlColType::Text { .. }
                | MysqlColType::LongText { .. } => "String",

                MysqlColType::Binary { .. }
                | MysqlColType::VarBinary { .. }
                | MysqlColType::TinyBlob
                | MysqlColType::MediumBlob
                | MysqlColType::Blob
                | MysqlColType::LongBlob => "BINARY",

                MysqlColType::Bit => "VARCHAR",
                MysqlColType::Set { items: _ } => "VARCHAR",
                MysqlColType::Enum { items: _ } => "VARCHAR",
                MysqlColType::Json => "JSON",
                MysqlColType::Unkown => "STRING",
            };
            ck_cols.push(format!("`{}` {}", col, ck_col_type));
        }

        ck_cols.push(format!("`{}` {}", SIGN_COL_NAME, SIGN_COL_TYPE));
        ck_cols.push(format!("`{}` {}", VERSION_COL_NAME, VERSION_COL_TYPE));

        // engine, default: ReplacingMergeTree
        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({})",
            table.database_name,
            table.table_name,
            ck_cols.join(", "),
        );

        if !tb_meta.basic.id_cols.is_empty() {
            let primary_keys = tb_meta
                .basic
                .id_cols
                .iter()
                .map(|i| format!("`{}`", i))
                .collect::<Vec<String>>()
                .join(",");
            sql = format!(
                "{} PRIMARY KEY ({}) DISTRIBUTED BY HASH(`{}`)",
                sql, primary_keys, tb_meta.basic.id_cols[0]
            );
        }

        if backend_count < 3 {
            sql = format!(r#"{} PROPERTIES ("replication_num" = "1")"#, sql);
        }
        Ok(sql)
    }

    async fn get_backend_count(&self) -> anyhow::Result<i32> {
        let sql = "SHOW BACKENDS";
        let mut count = 0;
        let mut rows = sqlx::query(sql).disable_arguments().fetch(&self.conn_pool);
        while (rows.try_next().await?).is_some() {
            count += 1;
        }
        Ok(count)
    }

    async fn execute_sql(&self, sql: &str) -> anyhow::Result<()> {
        log_info!("ddl begin: {}", sql);
        let query = sqlx::query(sql).disable_arguments();
        match query.execute(&self.conn_pool).await {
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
