use std::collections::HashMap;

use dt_common::{error::Error, utils::rdb_filter::RdbFilter};
use dt_meta::struct_meta::{
    statement::{
        pg_create_schema_statement::PgCreateSchemaStatement,
        pg_create_table_statement::PgCreateTableStatement,
    },
    structure::{
        column::Column,
        comment::{Comment, CommentType},
        constraint::Constraint,
        database::Database,
        index::{Index, IndexKind},
        sequence::Sequence,
        sequence_owner::SequenceOwner,
        table::Table,
    },
};
use futures::TryStreamExt;
use sqlx::{postgres::PgRow, Pool, Postgres, Row};

pub struct PgStructFetcher {
    pub conn_pool: Pool<Postgres>,
    pub schema: String,
    pub filter: Option<RdbFilter>,
}

enum ColType {
    Text,
    Char,
}

impl PgStructFetcher {
    pub async fn get_create_database_statement(
        &mut self,
    ) -> Result<PgCreateSchemaStatement, Error> {
        let database = self.get_database().await.unwrap();
        Ok(PgCreateSchemaStatement { database })
    }

    pub async fn get_create_table_statements(
        &mut self,
        tb: &str,
    ) -> Result<Vec<PgCreateTableStatement>, Error> {
        let mut results = Vec::new();

        let tables = self.get_tables(tb).await.unwrap();
        let mut sequences = self.get_sequences(tb).await.unwrap();
        let mut sequence_owners = self.get_sequence_owners(tb).await.unwrap();
        let mut constraints = self.get_constraints(tb).await.unwrap();
        let mut indexes = self.get_indexes(tb).await.unwrap();
        let mut column_comments = self.get_column_comments(tb).await.unwrap();
        let mut table_comments = self.get_table_comments(tb).await.unwrap();

        for (table_name, table) in tables {
            let statement = PgCreateTableStatement {
                table,
                sequences: self.get_result(&mut sequences, &table_name),
                sequence_owners: self.get_result(&mut sequence_owners, &table_name),
                constraints: self.get_result(&mut constraints, &table_name),
                indexes: self.get_result(&mut indexes, &table_name),
                column_comments: self.get_result(&mut column_comments, &table_name),
                table_comments: self.get_result(&mut table_comments, &table_name),
            };
            results.push(statement);
        }
        Ok(results)
    }

    async fn get_database(&mut self) -> Result<Database, Error> {
        let sql = format!(
            "SELECT schema_name 
            FROM information_schema.schemata
            WHERE schema_name='{}'",
            self.schema
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await.unwrap() {
            let schema_name = Self::get_str_with_null(&row, "schema_name").unwrap();
            let database = Database { name: schema_name };
            return Ok(database);
        }

        return Err(Error::StructError(format!(
            "schema: {} not found",
            self.schema
        )));
    }

    async fn get_sequences(&mut self, tb: &str) -> Result<HashMap<String, Vec<Sequence>>, Error> {
        let mut results: HashMap<String, Vec<Sequence>> = HashMap::new();

        let tb_filter = if !tb.is_empty() {
            format!("AND tab.relname = '{}'", tb)
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT obj.sequence_catalog,
                obj.sequence_schema,
                tab.relname AS table_name,
                obj.sequence_name,
                obj.data_type,
                obj.start_value,
                obj.minimum_value,
                obj.maximum_value,
                obj.increment,
                obj.cycle_option
            FROM information_schema.sequences obj
            JOIN pg_class AS seq
                ON (seq.relname = obj.sequence_name)
            JOIN pg_namespace ns
                ON (seq.relnamespace = ns.oid)
            JOIN pg_depend AS dep
                ON (seq.relfilenode = dep.objid)
            JOIN pg_class AS tab
                ON (dep.refobjid = tab.relfilenode)
            WHERE ns.nspname='{}' 
            AND obj.sequence_schema='{}' {} 
            AND dep.deptype='a'",
            &self.schema, &self.schema, tb_filter
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let (sequence_schema, table_name, sequence_name): (String, String, String) = (
                Self::get_str_with_null(&row, "sequence_schema").unwrap(),
                Self::get_str_with_null(&row, "table_name").unwrap(),
                Self::get_str_with_null(&row, "sequence_name").unwrap(),
            );

            let sequence = Sequence {
                sequence_name,
                database_name: Self::get_str_with_null(&row, "sequence_catalog").unwrap(),
                schema_name: sequence_schema,
                data_type: Self::get_str_with_null(&row, "data_type").unwrap(),
                start_value: row.get("start_value"),
                increment: row.get("increment"),
                min_value: row.get("minimum_value"),
                max_value: row.get("maximum_value"),
                is_circle: Self::get_str_with_null(&row, "cycle_option").unwrap(),
            };
            self.push_to_results(&mut results, &table_name, sequence);
        }

        Ok(results)
    }

    async fn get_sequence_owners(
        &mut self,
        tb: &str,
    ) -> Result<HashMap<String, Vec<SequenceOwner>>, Error> {
        let mut results = HashMap::new();

        let tb_filter = if !tb.is_empty() {
            format!("AND tab.relname = '{}'", tb)
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT seq.relname,
                tab.relname AS table_name,
                attr.attname AS column_name,
                ns.nspname
            FROM pg_class AS seq
            JOIN pg_namespace ns
                ON (seq.relnamespace = ns.oid)
            JOIN pg_depend AS dep
                ON (seq.relfilenode = dep.objid)
            JOIN pg_class AS tab
                ON (dep.refobjid = tab.relfilenode)
            JOIN pg_attribute AS attr
                ON (attr.attnum = dep.refobjsubid AND attr.attrelid = dep.refobjid)
            WHERE dep.deptype='a'
                AND seq.relkind='S'
                AND ns.nspname = '{}' {}",
            &self.schema, tb_filter
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name, seq_name): (String, String, String) = (
                Self::get_str_with_null(&row, "nspname").unwrap(),
                Self::get_str_with_null(&row, "table_name").unwrap(),
                Self::get_str_with_null(&row, "relname").unwrap(),
            );

            let sequence_owner = SequenceOwner {
                sequence_name: seq_name,
                database_name: String::new(),
                schema_name,
                owner_table_name: table_name.clone(),
                owner_table_column_name: Self::get_str_with_null(&row, "column_name").unwrap(),
            };
            self.push_to_results(&mut results, &table_name, sequence_owner);
        }

        Ok(results)
    }

    async fn get_tables(&mut self, tb: &str) -> Result<HashMap<String, Table>, Error> {
        let mut results: HashMap<String, Table> = HashMap::new();

        let tb_filter = if !tb.is_empty() {
            format!("AND table_name = '{}'", tb)
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT table_schema,
                table_name,
                column_name,
                data_type,
                udt_name,
                character_maximum_length,
                is_nullable,
                column_default,
                numeric_precision,
                numeric_scale,
                is_identity,
                identity_generation,
                ordinal_position
            FROM information_schema.columns c
            WHERE table_schema ='{}' {}",
            &self.schema, tb_filter
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let (table_schema, table_name) = (
                Self::get_str_with_null(&row, "table_schema").unwrap(),
                Self::get_str_with_null(&row, "table_name").unwrap(),
            );

            if self.filter_tb(&table_name) {
                continue;
            }

            let order: i32 = row.try_get("ordinal_position").unwrap();
            let column = Column {
                column_name: Self::get_str_with_null(&row, "column_name").unwrap(),
                order_position: order as u32,
                default_value: row.get("column_default"),
                is_nullable: Self::get_str_with_null(&row, "is_nullable").unwrap(),
                column_type: Self::get_col_data_type(
                    Self::get_str_with_null(&row, "udt_name").unwrap(),
                    Self::get_str_with_null(&row, "data_type").unwrap(),
                    table_schema.clone(),
                    row.get("character_maximum_length"),
                    row.get("numeric_precision"),
                    row.get("numeric_scale"),
                )
                .unwrap(),
                generated: Self::get_col_generated_rule(
                    row.get("is_identity"),
                    row.get("identity_generation"),
                ),
                column_key: String::new(),
                extra: String::new(),
                column_comment: String::new(),
                character_set: String::new(),
                collation: String::new(),
            };

            if let Some(table) = results.get_mut(&table_name) {
                table.columns.push(column);
            } else {
                results.insert(
                    table_name.clone(),
                    Table {
                        database_name: table_schema.clone(),
                        schema_name: table_schema,
                        table_name,
                        engine_name: String::new(),
                        table_comment: String::new(),
                        columns: vec![column],
                    },
                );
            }
        }

        Ok(results)
    }

    async fn get_constraints(
        &mut self,
        tb: &str,
    ) -> Result<HashMap<String, Vec<Constraint>>, Error> {
        let mut results = HashMap::new();

        let tb_filter = if !tb.is_empty() {
            format!("AND rel.relname = '{}'", tb)
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT nsp.nspname,
                rel.relname,
                con.conname AS constraint_name,
                con.contype AS constraint_type,
                pg_get_constraintdef(con.oid) AS constraint_definition
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class rel
                ON rel.oid = con.conrelid
            JOIN pg_catalog.pg_namespace nsp
                ON nsp.oid = connamespace
            WHERE nsp.nspname ='{}' {}
            ORDER BY nsp.nspname,rel.relname",
            &self.schema, tb_filter
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name, constraint_name) = (
                Self::get_str_with_null(&row, "nspname").unwrap(),
                Self::get_str_with_null(&row, "relname").unwrap(),
                Self::get_str_with_null(&row, "constraint_name").unwrap(),
            );

            let constraint = Constraint {
                database_name: String::new(),
                schema_name,
                table_name: table_name.clone(),
                constraint_name,
                constraint_type: Self::get_with_null(&row, "constraint_type", ColType::Char)
                    .unwrap(),
                definition: Self::get_str_with_null(&row, "constraint_definition").unwrap(),
            };
            self.push_to_results(&mut results, &table_name, constraint);
        }

        Ok(results)
    }

    async fn get_indexes(&mut self, tb: &str) -> Result<HashMap<String, Vec<Index>>, Error> {
        let mut results = HashMap::new();

        let tb_filter = if !tb.is_empty() {
            format!("AND tablename = '{}'", tb)
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT schemaname,
                tablename,
                indexdef,
                COALESCE(tablespace, 'pg_default') AS tablespace, indexname
            FROM pg_indexes
            WHERE schemaname = '{}' {}",
            &self.schema, tb_filter
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name, index_name) = (
                Self::get_str_with_null(&row, "schemaname").unwrap(),
                Self::get_str_with_null(&row, "tablename").unwrap(),
                Self::get_str_with_null(&row, "indexname").unwrap(),
            );

            let index = Index {
                database_name: String::new(),
                schema_name,
                table_name: table_name.clone(),
                index_name,
                index_kind: IndexKind::Index,
                index_type: String::new(),
                comment: String::new(),
                tablespace: Self::get_str_with_null(&row, "tablespace").unwrap(),
                definition: Self::get_str_with_null(&row, "indexdef").unwrap(),
                columns: Vec::new(),
            };
            self.push_to_results(&mut results, &table_name, index);
        }

        Ok(results)
    }

    async fn get_table_comments(
        &mut self,
        tb: &str,
    ) -> Result<HashMap<String, Vec<Comment>>, Error> {
        let mut results = HashMap::new();

        let tb_filter = if !tb.is_empty() {
            format!("AND c.relname = '{}'", tb)
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT n.nspname,
                c.relname,
                d.description
            FROM pg_class c
            LEFT JOIN pg_namespace n
                ON n.oid = c.relnamespace
            LEFT JOIN pg_description d
                ON c.oid = d.objoid  AND d.objsubid = 0
            WHERE n.nspname ='{}' {}
            AND d.description IS NOT null",
            &self.schema, tb_filter
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name): (String, String) = (
                Self::get_str_with_null(&row, "nspname").unwrap(),
                Self::get_str_with_null(&row, "relname").unwrap(),
            );

            let comment = Comment {
                comment_type: CommentType::Table,
                database_name: String::new(),
                schema_name,
                table_name: table_name.clone(),
                column_name: String::new(),
                comment: Self::get_str_with_null(&row, "description").unwrap(),
            };
            self.push_to_results(&mut results, &table_name, comment);
        }

        Ok(results)
    }

    async fn get_column_comments(
        &mut self,
        tb: &str,
    ) -> Result<HashMap<String, Vec<Comment>>, Error> {
        let mut results = HashMap::new();

        let tb_filter = if !tb.is_empty() {
            format!("AND c.relname = '{}'", tb)
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT n.nspname,
                c.relname,
                col_description(a.attrelid, a.attnum) as comment,
                format_type(a.atttypid, a.atttypmod)as type,
                a.attname AS name,
                a.attnotnull AS notnull
            FROM pg_class c
            LEFT JOIN pg_attribute a
                ON a.attrelid =c.oid
            LEFT JOIN pg_namespace n
                ON n.oid = c.relnamespace
            WHERE n.nspname ='{}' {}
                AND a.attnum >0
                AND col_description(a.attrelid, a.attnum) is NOT null",
            &self.schema, tb_filter
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name, column_name) = (
                Self::get_str_with_null(&row, "nspname").unwrap(),
                Self::get_str_with_null(&row, "relname").unwrap(),
                Self::get_str_with_null(&row, "name").unwrap(),
            );

            let comment = Comment {
                comment_type: CommentType::Column,
                database_name: String::new(),
                schema_name,
                table_name: table_name.clone(),
                column_name,
                comment: Self::get_str_with_null(&row, "comment").unwrap(),
            };
            self.push_to_results(&mut results, &table_name, comment);
        }

        Ok(results)
    }

    fn get_str_with_null(row: &PgRow, col_name: &str) -> Result<String, Error> {
        Self::get_with_null(row, col_name, ColType::Text)
    }

    fn get_with_null(row: &PgRow, col_name: &str, col_type: ColType) -> Result<String, Error> {
        let mut str_val = String::new();
        match col_type {
            ColType::Text => {
                let str_val_option: Option<String> = row.get(col_name);
                if let Some(s) = str_val_option {
                    str_val = s
                }
            }
            ColType::Char => {
                let char_val: i8 = row.get(col_name);
                str_val = char_val.to_string();
            }
        }
        Ok(str_val)
    }

    fn get_col_data_type(
        udt_name: String,
        data_type: String,
        schema_name: String,
        char_max_length: Option<i32>,
        num_percision: Option<i32>,
        num_scale: Option<i32>,
    ) -> Option<String> {
        let mut result_type = String::new();
        let type_vec = vec![
            "geometry",
            "box2d",
            "box2df",
            "box3d",
            "geography",
            "geometry_dump",
            "gidx",
            "spheroid",
            "valid_detail",
            "_text",
        ];
        if type_vec.contains(&udt_name.as_str()) {
            result_type.push_str(udt_name.as_str());
        } else if data_type == "USER-DEFINED" {
            result_type.push_str(format!("{}.{}", schema_name, udt_name).as_str());
        } else {
            result_type.push_str(data_type.as_str());
        }
        if char_max_length.is_some() {
            result_type.push_str(format!("({})", char_max_length.unwrap()).as_str());
        } else if num_percision.is_some()
            && num_percision.unwrap() > 0
            && num_scale.is_some()
            && num_scale.unwrap() > 0
        {
            result_type
                .push_str(format!("({},{})", num_percision.unwrap(), num_scale.unwrap()).as_str())
        }
        Some(result_type)
    }

    fn get_col_generated_rule(
        is_identity: Option<String>,
        identity_generation: Option<String>,
    ) -> Option<String> {
        if let Some(i) = is_identity {
            if i.to_lowercase() == "yes" && identity_generation.is_some() {
                return identity_generation;
            }
        }
        None
    }

    fn filter_tb(&mut self, tb: &str) -> bool {
        if let Some(filter) = &mut self.filter {
            return filter.filter_tb(&self.schema, &tb);
        }
        false
    }

    fn push_to_results<T>(
        &mut self,
        results: &mut HashMap<String, Vec<T>>,
        table_name: &str,
        item: T,
    ) {
        if self.filter_tb(&table_name) {
            return;
        }

        if let Some(exists) = results.get_mut(table_name) {
            exists.push(item);
        } else {
            results.insert(table_name.into(), vec![item]);
        }
    }

    fn get_result<T>(&self, results: &mut HashMap<String, Vec<T>>, table_name: &str) -> Vec<T> {
        if let Some(result) = results.remove(table_name) {
            result
        } else {
            Vec::new()
        }
    }
}
