use crate::{close_conn_pool, Sinker};

use dt_common::{config::config_enums::ConflictPolicyEnum, error::Error};

use dt_meta::{
    ddl_data::DdlData,
    row_data::RowData,
    struct_meta::{
        database_model::{Column, StructModel},
        pg_enums::ConstraintTypeEnum,
    },
};

use async_trait::async_trait;
use sqlx::{query, Pool, Postgres};

#[derive(Clone)]
pub struct PgStructSinker {
    pub conn_pool: Pool<Postgres>,
    pub conflict_policy: ConflictPolicyEnum,
}

#[async_trait]
impl Sinker for PgStructSinker {
    async fn sink_dml(&mut self, mut _data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        return close_conn_pool!(self);
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        for ddl_data in data {
            let mut model = ddl_data.meta.unwrap();
            let result = self.sink_internal(&mut model).await;
            match self.conflict_policy {
                ConflictPolicyEnum::Ignore => {}
                ConflictPolicyEnum::Interrupt => result.unwrap(),
            }
        }
        Ok(())
    }
}

impl PgStructSinker {
    async fn sink_internal(&self, model: &mut StructModel) -> Result<(), Error> {
        match model {
            StructModel::TableModel {
                schema_name,
                table_name,
                columns,
                ..
            } => {
                let col_sql = PgStructSinker::build_columns_sql_str(columns).unwrap();
                let sql = format!(
                    "CREATE TABLE \"{}\".\"{}\" ({})",
                    schema_name, table_name, col_sql
                );
                match query(&sql).execute(&self.conn_pool).await {
                    Ok(_) => {
                        return {
                            println!("create table sql:[{}],execute success", sql);
                            Ok(())
                        }
                    }
                    Err(e) => {
                        return {
                            println!(
                                "create table sql:[{}],execute failed:{}",
                                sql,
                                e.to_string()
                            );
                            Err(Error::from(e))
                        }
                    }
                }
            }
            StructModel::IndexModel {
                tablespace,
                definition,
                ..
            } => {
                let sql = format!(
                    "{} TABLESPACE {}",
                    definition
                        .replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS")
                        .replace("CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX IF NOT EXISTS"),
                    tablespace
                );
                match query(&sql).execute(&self.conn_pool).await {
                    Ok(_) => {
                        return {
                            println!("add index sql:[{}],execute success", sql);
                            Ok(())
                        }
                    }
                    Err(e) => {
                        return {
                            println!("add index sql:[{}],execute failed:{}", sql, e.to_string());
                            Err(Error::from(e))
                        }
                    }
                }
            }
            StructModel::ConstraintModel {
                schema_name,
                table_name,
                constraint_name,
                constraint_type,
                definition,
                ..
            } => {
                if constraint_type == &ConstraintTypeEnum::Foregin.to_charval().unwrap() {
                    let msg = format!("foreign key is not supported yet, schema:[{}], table:[{}], constraint_name:[{}]", schema_name, table_name, constraint_name);
                    println!("{}", msg);
                    return Err(Error::StructError { error: msg });
                }
                let sql = format!(
                    "ALTER TABLE \"{}\".\"{}\" ADD CONSTRAINT \"{}\" {}",
                    schema_name, table_name, constraint_name, definition
                );
                match query(&sql).execute(&self.conn_pool).await {
                    Ok(_) => {
                        return {
                            println!("add costraint sql:[{}],execute success", sql);
                            Ok(())
                        }
                    }
                    Err(e) => {
                        return {
                            println!(
                                "add costraint sql:[{}],execute failed:{}",
                                sql,
                                e.to_string()
                            );
                            Err(Error::from(e))
                        }
                    }
                }
            }
            StructModel::SequenceModel {
                sequence_name,
                schema_name,
                data_type,
                start_value,
                increment,
                min_value,
                max_value,
                is_circle,
                ..
            } => {
                let mut cycle_str = String::from("NO CYCLE");
                if is_circle.to_lowercase() == "yes" {
                    cycle_str = String::from("CYCLE");
                }
                let create_sql = format!("CREATE SEQUENCE \"{}\".\"{}\" AS {} START {} INCREMENT by {} MINVALUE {} MAXVALUE {} {}", schema_name, sequence_name, data_type, start_value, increment, min_value, max_value, cycle_str);
                match query(&create_sql).execute(&self.conn_pool).await {
                    Ok(_) => {
                        return {
                            println!(
                                "add sequence:[{}], sql:[{}],execute success",
                                sequence_name, create_sql
                            );
                            Ok(())
                        }
                    }
                    Err(e) => {
                        return {
                            println!(
                                "add sequence:[{}], sql:[{}],execute failed:{}",
                                sequence_name,
                                create_sql,
                                e.to_string()
                            );
                            Err(Error::from(e))
                        }
                    }
                }
            }
            StructModel::SequenceOwnerModel {
                sequence_name,
                schema_name,
                owner_table_name,
                owner_table_column_name,
                ..
            } => {
                if !owner_table_name.is_empty() && !owner_table_column_name.is_empty() {
                    let create_owner_sql = format!(
                        "ALTER SEQUENCE \"{}\".\"{}\" OWNED BY \"{}\".\"{}\".\"{}\"",
                        schema_name,
                        sequence_name,
                        schema_name,
                        owner_table_name,
                        owner_table_column_name
                    );
                    match query(&create_owner_sql).execute(&self.conn_pool).await {
                        Ok(_) => {
                            return {
                                println!(
                                    "add ownership for sequence:[{}], sql:[{}], execute success",
                                    sequence_name, create_owner_sql
                                );
                                Ok(())
                            }
                        }
                        Err(e) => {
                            return {
                                println!(
                                    "add ownership for sequence:[{}], sql:[{}], execute success,execute failed:{}",
                                    sequence_name,
                                    create_owner_sql,
                                    e.to_string()
                                );
                                Err(Error::from(e))
                            }
                        }
                    }
                } else {
                    println!(
                        "schema:[{}.{}] has no ownership.",
                        schema_name, sequence_name
                    );
                }
            }
            StructModel::CommentModel {
                schema_name,
                table_name,
                column_name,
                comment,
                ..
            } => {
                if (table_name.is_empty() && column_name.is_empty()) || comment.is_empty() {
                    return Ok(());
                }
                let sql;
                if !column_name.is_empty() {
                    sql = format!(
                        "COMMENT ON COLUMN \"{}\".\"{}\".\"{}\" IS '{}'",
                        schema_name, table_name, column_name, comment
                    )
                } else {
                    sql = format!(
                        "COMMENT ON TABLE \"{}\".\"{}\" is '{}'",
                        schema_name, table_name, comment
                    )
                }
                match query(&sql).execute(&self.conn_pool).await {
                    Ok(_) => {
                        return {
                            println!("create comment sql:[{}],execute success", sql);
                            Ok(())
                        }
                    }
                    Err(e) => {
                        return {
                            println!(
                                "create comment sql:[{}],execute failed:{}",
                                sql,
                                e.to_string()
                            );
                            Err(Error::from(e))
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn build_columns_sql_str(columns: &mut Vec<Column>) -> Option<String> {
        let mut result_str = String::from("");
        columns.sort_by(|a, b| a.order_position.cmp(&b.order_position));
        for column in columns {
            result_str
                .push_str(format!("\"{}\" {} ", column.column_name, column.column_type).as_str());
            if column.is_nullable.to_lowercase() == "no" {
                result_str.push_str("NOT NULL ");
            }
            match &column.default_value {
                Some(x) => result_str.push_str(format!("DEFAULT {} ", x).as_str()),
                None => {}
            }
            match &column.generated {
                Some(x) => {
                    if x == "ALWAYS" {
                        result_str.push_str("GENERATED ALWAYS AS IDENTITY ")
                    } else {
                        result_str.push_str("GENERATED BY DEFAULT AS IDENTITY ")
                    }
                }
                None => {}
            }
            result_str.push_str(",");
        }
        if result_str.ends_with(",") {
            result_str = result_str[0..result_str.len() - 1].to_string();
        }
        Some(result_str)
    }
}
