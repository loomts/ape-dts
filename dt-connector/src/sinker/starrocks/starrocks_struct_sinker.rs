use crate::{rdb_router::RdbRouter, Sinker};

use anyhow::bail;
use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    log_error, log_info,
    meta::{
        mysql::{mysql_col_type::MysqlColType, mysql_tb_meta::MysqlTbMeta},
        pg::{pg_tb_meta::PgTbMeta, pg_value_type::PgValueType},
        rdb_meta_manager::RdbMetaManager,
        struct_meta::{
            statement::struct_statement::StructStatement,
            struct_data::StructData,
            structure::{column::Column, table::Table},
        },
    },
    rdb_filter::RdbFilter,
};

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};

const SIGN_COL_NAME: &str = "_ape_dts_is_deleted";
const SIGN_COL_TYPE: &str = "BOOLEAN";
const TIMESTAMP_COL_NAME: &str = "_ape_dts_timestamp";
const TIMESTAMP_COL_TYPE: &str = "BIGINT";

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
                        let tb_meta = meta_manager.get_tb_meta(schema, tb).await?.to_owned();
                        let sql =
                            self.get_create_table_sql(&statement.table, Some(&tb_meta), None)?;
                        self.execute_sql(&sql).await?;
                    }
                }

                StructStatement::PgCreateSchema(statement) => {
                    let sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", statement.schema.name);
                    self.execute_sql(&sql).await?;
                }

                StructStatement::PgCreateTable(statement) => {
                    let schema = &statement.table.schema_name;
                    let tb = &statement.table.table_name;
                    if let Some(meta_manager) = self.extractor_meta_manager.pg_meta_manager.as_mut()
                    {
                        let tb_meta = meta_manager.get_tb_meta(schema, tb).await?.to_owned();
                        let sql =
                            self.get_create_table_sql(&statement.table, None, Some(&tb_meta))?;
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
    fn get_create_table_sql(
        &self,
        table: &Table,
        mysql_tb_meta: Option<&MysqlTbMeta>,
        pg_tb_meta: Option<&PgTbMeta>,
    ) -> anyhow::Result<String> {
        let rdb_tb_meta = if let Some(tb_meta) = pg_tb_meta {
            &tb_meta.basic
        } else {
            &mysql_tb_meta.as_ref().unwrap().basic
        };

        let mut dst_cols = vec![];

        // In StarRocks:
        // Key columns must be the first few columns of the schema and the order
        // of the key columns must be consistent with the order of the schema.
        for col in rdb_tb_meta.id_cols.iter() {
            for column in table.columns.iter() {
                if column.column_name == *col {
                    dst_cols.push(Self::get_dst_col(column, mysql_tb_meta, pg_tb_meta)?);
                }
            }
        }

        for column in table.columns.iter() {
            if !rdb_tb_meta.id_cols.contains(&column.column_name) {
                dst_cols.push(Self::get_dst_col(column, mysql_tb_meta, pg_tb_meta)?);
            }
        }

        // sign and timestamp cols
        dst_cols.push(format!("`{}` {}", SIGN_COL_NAME, SIGN_COL_TYPE));
        dst_cols.push(format!("`{}` {}", TIMESTAMP_COL_NAME, TIMESTAMP_COL_TYPE));

        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({})",
            rdb_tb_meta.schema,
            rdb_tb_meta.tb,
            dst_cols.join(", "),
        );

        if !rdb_tb_meta.id_cols.is_empty() {
            let primary_keys = rdb_tb_meta
                .id_cols
                .iter()
                .map(|i| format!("`{}`", i))
                .collect::<Vec<String>>()
                .join(",");
            sql = format!("{} PRIMARY KEY ({})", sql, primary_keys);
            if !table.table_comment.is_empty() {
                sql = format!("{} COMMENT '{}'", sql, table.table_comment);
            }
            sql = format!("{} DISTRIBUTED BY HASH(`{}`)", sql, rdb_tb_meta.id_cols[0]);
        }

        if self.backend_count < 3 {
            sql = format!(r#"{} PROPERTIES ("replication_num" = "1")"#, sql);
        }
        Ok(sql)
    }

    fn get_dst_col(
        column: &Column,
        mysql_tb_meta: Option<&MysqlTbMeta>,
        pg_tb_meta: Option<&PgTbMeta>,
    ) -> anyhow::Result<String> {
        let col = &column.column_name;
        let dst_col_type = if let Some(tb_meta) = mysql_tb_meta {
            Self::get_dst_col_type_from_mysql(col, tb_meta)
        } else {
            Self::get_dst_col_type_from_pg(col, pg_tb_meta.unwrap())
        }?;

        let mut dst_col = if column.is_nullable {
            format!("`{}` {}", col, dst_col_type)
        } else {
            format!("`{}` {} NOT NULL", col, dst_col_type)
        };

        if !column.column_comment.is_empty() {
            dst_col = format!("{} COMMENT='{}'", dst_col, column.column_comment);
        }

        Ok(dst_col)
    }

    fn get_dst_col_type_from_mysql(col: &str, tb_meta: &MysqlTbMeta) -> anyhow::Result<String> {
        let mysql_col_type = tb_meta.get_col_type(col)?;
        let dst_col = match mysql_col_type {
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

            MysqlColType::Time => "VARCHAR(255)",
            MysqlColType::Date => "DATE",
            MysqlColType::DateTime => "DATETIME",
            MysqlColType::Timestamp { timezone_offset: _ } => "DATETIME",
            MysqlColType::Year => "INT",

            MysqlColType::Char { length: v, .. } => format!("CHAR({})", v).leak(),
            MysqlColType::Varchar { length: v, .. } => format!("VARCHAR({})", v).leak(),

            MysqlColType::TinyText { .. }
            | MysqlColType::MediumText { .. }
            | MysqlColType::Text { .. }
            | MysqlColType::LongText { .. } => "STRING",

            MysqlColType::Binary { .. }
            | MysqlColType::VarBinary { .. }
            | MysqlColType::TinyBlob
            | MysqlColType::MediumBlob
            | MysqlColType::Blob
            | MysqlColType::LongBlob => "VARBINARY",

            MysqlColType::Bit => "BIGINT",
            MysqlColType::Set { items: _ } => "VARCHAR(255)",
            MysqlColType::Enum { items: _ } => "VARCHAR(255)",
            MysqlColType::Json => "JSON",
            MysqlColType::Unknown => "STRING",
        };
        Ok(dst_col.to_string())
    }

    fn get_dst_col_type_from_pg(col: &str, tb_meta: &PgTbMeta) -> anyhow::Result<String> {
        let pg_col_type = tb_meta.get_col_type(col)?;
        let dst_col = match pg_col_type.value_type {
            // boolean == tinyint(1)
            PgValueType::Boolean => "BOOLEAN",
            PgValueType::Int16 => "SMALLINT",
            PgValueType::Int32 => "INT",
            PgValueType::Int64 => "BIGINT",
            PgValueType::Float32 => "FLOAT",
            PgValueType::Float64 => "DOUBLE",

            // TODO, set precision / scale according to source
            PgValueType::Numeric => "DECIMAL(38,9)",
            PgValueType::Char => "CHAR",
            PgValueType::String => "STRING",
            PgValueType::JSON => "JSON",

            PgValueType::Time | PgValueType::TimeTZ | PgValueType::Interval => "VARCHAR(255)",
            PgValueType::Timestamp | PgValueType::TimestampTZ => "DATETIME",
            PgValueType::Date => "DATE",

            PgValueType::Bytes => "VARBINARY",
            _ => "STRING",
        };
        Ok(dst_col.to_string())
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
