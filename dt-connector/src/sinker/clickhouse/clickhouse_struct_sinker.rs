use crate::{rdb_router::RdbRouter, Sinker};

use anyhow::bail;
use clickhouse::Client;
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

const SIGN_COL_NAME: &str = "_ape_dts_is_deleted";
const SIGN_COL_TYPE: &str = "Int8";
const VERSION_COL_NAME: &str = "_ape_dts_version";
const VERSION_COL_TYPE: &str = "Int64";

#[derive(Clone)]
pub struct ClickhouseStructSinker {
    pub client: Client,
    pub conflict_policy: ConflictPolicyEnum,
    pub engine: String,
    pub filter: RdbFilter,
    pub router: RdbRouter,
    pub extractor_meta_manager: RdbMetaManager,
}

#[async_trait]
impl Sinker for ClickhouseStructSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
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
                        let sql = Self::get_create_table_sql_from_mysql(&statement.table, tb_meta)?;
                        self.execute_sql(&sql).await?;
                    }
                }

                _ => {}
            }
        }

        Ok(())
    }
}

impl ClickhouseStructSinker {
    fn get_create_table_sql_from_mysql(
        table: &Table,
        tb_meta: &MysqlTbMeta,
    ) -> anyhow::Result<String> {
        let mut ck_cols = vec![];
        for column in table.columns.iter() {
            let col = &column.column_name;
            let mysql_col_type = tb_meta.get_col_type(col)?;
            let ck_col_type = match mysql_col_type {
                MysqlColType::TinyInt { unsigned: false } => "Int8",
                MysqlColType::TinyInt { unsigned: true } => "UInt8",
                MysqlColType::SmallInt { unsigned: false } => "Int16",
                MysqlColType::SmallInt { unsigned: true } => "UInt16",
                MysqlColType::MediumInt { unsigned: false } => "Int32",
                MysqlColType::MediumInt { unsigned: true } => "UInt32",
                MysqlColType::Int { unsigned: false } => "Int32",
                MysqlColType::Int { unsigned: true } => "UInt32",
                MysqlColType::BigInt { unsigned: false } => "Int64",
                MysqlColType::BigInt { unsigned: true } => "UInt64",

                MysqlColType::Float => "Float32",
                MysqlColType::Double => "Float64",
                MysqlColType::Decimal { precision, scale } => {
                    format!("Decimal({},{})", precision, scale).leak()
                }

                MysqlColType::Time => "String",
                MysqlColType::Date => "Date32",
                MysqlColType::DateTime => "DateTime64(6)",
                MysqlColType::Timestamp { timezone_offset: _ } => "DateTime64(6)",
                MysqlColType::Year => "Int32",

                MysqlColType::Char { .. }
                | MysqlColType::Varchar { .. }
                | MysqlColType::TinyText { .. }
                | MysqlColType::MediumText { .. }
                | MysqlColType::Text { .. }
                | MysqlColType::LongText { .. } => "String",

                MysqlColType::Binary { length: _ } => "String",
                MysqlColType::VarBinary { length: _ } => "String",
                MysqlColType::TinyBlob
                | MysqlColType::MediumBlob
                | MysqlColType::Blob
                | MysqlColType::LongBlob => "String",

                MysqlColType::Bit => "String",
                MysqlColType::Set { items: _ } => "String",
                MysqlColType::Enum { items: _ } => "String",
                MysqlColType::Json => "String",
                MysqlColType::Unknown => "String",
            };

            if column.is_nullable {
                ck_cols.push(format!("`{}` Nullable({})", col, ck_col_type));
            } else {
                ck_cols.push(format!("`{}` {}", col, ck_col_type));
            }
        }

        ck_cols.push(format!("`{}` {}", SIGN_COL_NAME, SIGN_COL_TYPE));
        ck_cols.push(format!("`{}` {}", VERSION_COL_NAME, VERSION_COL_TYPE));

        // engine, default: ReplacingMergeTree
        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({}) ENGINE = ReplacingMergeTree(`{}`)",
            table.database_name,
            table.table_name,
            ck_cols.join(", "),
            VERSION_COL_NAME
        );

        if !tb_meta.basic.id_cols.is_empty() {
            let order_by = tb_meta
                .basic
                .id_cols
                .iter()
                .map(|i| format!("`{}`", i))
                .collect::<Vec<String>>()
                .join(",");
            sql = format!("{} PRIMARY KEY ({}) ORDER BY ({})", sql, order_by, order_by);
        }
        Ok(sql)
    }

    async fn execute_sql(&self, sql: &str) -> anyhow::Result<()> {
        log_info!("ddl begin: {}", sql);
        match self.client.query(sql).execute().await {
            Ok(()) => {
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
