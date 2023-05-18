use std::{
    collections::{HashMap, HashSet},
    sync::atomic::AtomicBool,
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{error::Error, log_info};

use dt_meta::{
    ddl_data::DdlData,
    ddl_type::DdlType,
    dt_data::DtData,
    struct_meta::{
        col_model::ColType,
        database_model::{Column, IndexKind, StructModel},
    },
};

use futures::TryStreamExt;
use sqlx::{postgres::PgRow, Pool, Postgres, Row};

use crate::{
    extractor::{base_extractor::BaseExtractor, rdb_filter::RdbFilter},
    Extractor,
};

pub struct PgStructExtractor<'a> {
    pub conn_pool: Pool<Postgres>,
    pub buffer: &'a ConcurrentQueue<DtData>,
    pub db: String,
    pub filter: RdbFilter,
    pub shut_down: &'a AtomicBool,
}

#[async_trait]
impl Extractor for PgStructExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!("PgStructExtractor starts, schema: {}", self.db,);
        self.extract_internal().await
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl PgStructExtractor<'_> {
    pub async fn extract_internal(&mut self) -> Result<(), Error> {
        let mut metas = Vec::new();

        let (seqs, seq_owners) = self.get_sequence().await.unwrap();

        for (_, seq) in seqs {
            metas.push(seq);
        }

        for (_, table) in self.get_table().await.unwrap() {
            metas.push(table);
        }

        for (_, seq_owner) in seq_owners {
            metas.push(seq_owner);
        }

        for (_, constraint) in self.get_constraint().await.unwrap() {
            metas.push(constraint);
        }

        for (_, index) in self.get_index().await.unwrap() {
            metas.push(index);
        }

        for (_, comment) in self.get_comment().await.unwrap() {
            metas.push(comment);
        }

        for meta in metas {
            let ddl_data = DdlData {
                schema: self.db.clone(),
                query: String::new(),
                meta: Some(meta),
                ddl_type: DdlType::Unknown,
            };
            BaseExtractor::push_dt_data(&self.buffer, DtData::Ddl { ddl_data })
                .await
                .unwrap();
        }
        BaseExtractor::wait_task_finish(self.buffer, self.shut_down).await
    }

    pub async fn get_sequence(
        &mut self,
    ) -> Result<(HashMap<String, StructModel>, HashMap<String, StructModel>), Error> {
        let mut table_used_seqs = HashSet::new();
        let mut models = HashMap::new();
        let mut seq_owners = HashMap::new();

        // table
        let sql = format!(
            "SELECT c.table_schema,c.table_name,c.column_name, c.data_type, c.column_default  
            FROM information_schema.columns c 
            WHERE table_schema ='{}'
            AND column_default IS NOT null AND column_default like 'nextval(%'",
            self.db
        );
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name): (String, String) = (
                PgStructExtractor::get_str_with_null(&row, "table_schema").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "table_name").unwrap(),
            );

            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }

            if self.filter.filter_tb(&schema_name, &table_name) {
                continue;
            }

            // build with default_value, such as nextval('table_test_name_seq'::regclass), find seq name
            match PgStructExtractor::get_seq_name_by_default_value(
                PgStructExtractor::get_str_with_null(&row, "column_default").unwrap(),
            ) {
                Some(seq_name) => table_used_seqs.insert(seq_name),
                None => false,
            };
        }

        if table_used_seqs.len() > 0 {
            // query target seq
            let sql = format!("SELECT sequence_catalog,sequence_schema,sequence_name,data_type,start_value,minimum_value,maximum_value,increment,cycle_option 
                FROM information_schema.sequences 
                WHERE sequence_schema='{}'", self.db);
            let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

            while let Some(row) = rows.try_next().await.unwrap() {
                let seq_name = Self::get_str_with_null(&row, "sequence_name").unwrap();
                if !table_used_seqs.contains(&seq_name) {
                    continue;
                }

                let (schema_name, seq_name): (String, String) = (
                    PgStructExtractor::get_str_with_null(&row, "sequence_schema").unwrap(),
                    PgStructExtractor::get_str_with_null(&row, "sequence_name").unwrap(),
                );

                if schema_name.is_empty() || seq_name.is_empty() {
                    continue;
                }

                if self.filter.filter_db(&schema_name) {
                    continue;
                }

                let schema_seq_name = format!("{}.{}", schema_name, seq_name);
                models.insert(
                    schema_seq_name,
                    StructModel::SequenceModel {
                        sequence_name: seq_name,
                        database_name: Self::get_str_with_null(&row, "sequence_catalog").unwrap(),
                        schema_name: self.db.clone(),
                        data_type: Self::get_str_with_null(&row, "data_type").unwrap(),
                        start_value: row.get("start_value"),
                        increment: row.get("increment"),
                        min_value: row.get("minimum_value"),
                        max_value: row.get("maximum_value"),
                        is_circle: Self::get_str_with_null(&row, "cycle_option").unwrap(),
                    },
                );
            }

            // query seq ownership, and put the StructModel into memory, will push to queue after get_tables
            let sql = format!("select seq.relname,tab.relname as table_name, attr.attname as column_name, ns.nspname 
                from pg_class as seq 
                join pg_namespace ns on (seq.relnamespace = ns.oid) 
                join pg_depend as dep on (seq.relfilenode = dep.objid) 
                join pg_class as tab on (dep.refobjid = tab.relfilenode) 
                join pg_attribute as attr on (attr.attnum = dep.refobjsubid and attr.attrelid = dep.refobjid) 
                where dep.deptype='a' and ns.nspname = '{}' ", self.db);
            let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

            while let Some(row) = rows.try_next().await.unwrap() {
                let (schema_name, seq_name): (String, String) = (
                    PgStructExtractor::get_str_with_null(&row, "nspname").unwrap(),
                    PgStructExtractor::get_str_with_null(&row, "relname").unwrap(),
                );

                if schema_name.is_empty() && seq_name.is_empty() {
                    continue;
                }

                if self.filter.filter_db(&schema_name) {
                    continue;
                }

                let schema_seq_name = format!("{}.{}", schema_name, seq_name);
                if (!table_used_seqs.contains(&seq_name)) || !models.contains_key(&schema_seq_name)
                {
                    continue;
                }

                if !seq_owners.contains_key(&schema_seq_name) {
                    seq_owners.insert(
                        schema_seq_name,
                        StructModel::SequenceOwnerModel {
                            sequence_name: seq_name,
                            database_name: String::new(),
                            schema_name,
                            owner_table_name: Self::get_str_with_null(&row, "table_name").unwrap(),
                            owner_table_column_name: Self::get_str_with_null(&row, "column_name")
                                .unwrap(),
                        },
                    );
                }
            }
        }

        Ok((models, seq_owners))
    }

    pub async fn get_table(&mut self) -> Result<HashMap<String, StructModel>, Error> {
        let sql = format!("SELECT c.table_schema,c.table_name,c.column_name, c.data_type, c.udt_name, c.character_maximum_length, c.is_nullable, c.column_default, c.numeric_precision, c.numeric_scale, c.is_identity, c.identity_generation,c.ordinal_position 
            FROM information_schema.columns c 
            WHERE table_schema ='{}' 
            ORDER BY table_schema, table_name, column_name", self.db);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut results = HashMap::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name) = (
                PgStructExtractor::get_str_with_null(&row, "table_schema").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "table_name").unwrap(),
            );

            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }

            if self.filter.filter_tb(&schema_name, &table_name) {
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
                    schema_name.clone(),
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

            let schema_table_name = format!("{}.{}", schema_name, table_name);
            if let Some(model) = results.get_mut(&schema_table_name) {
                match model {
                    StructModel::TableModel { columns, .. } => {
                        columns.push(column);
                    }
                    _ => {}
                }
            } else {
                results.insert(
                    schema_table_name,
                    StructModel::TableModel {
                        database_name: schema_name.clone(),
                        schema_name,
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

    pub async fn get_constraint(&mut self) -> Result<HashMap<String, StructModel>, Error> {
        let sql = format!("SELECT nsp.nspname, rel.relname, con.conname as constraint_name, con.contype as constraint_type,pg_get_constraintdef(con.oid) as constraint_definition
            FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
            WHERE nsp.nspname ='{}' 
            ORDER BY nsp.nspname,rel.relname", self.db);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut result = HashMap::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name): (String, String) = (
                PgStructExtractor::get_str_with_null(&row, "nspname").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "relname").unwrap(),
            );

            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }

            if self.filter.filter_tb(&schema_name, &table_name) {
                continue;
            }

            let constraint_name = Self::get_str_with_null(&row, "constraint_name").unwrap();
            let full_constraint_name =
                format!("{}.{}.{}", schema_name, table_name, constraint_name);

            result.insert(
                full_constraint_name,
                StructModel::ConstraintModel {
                    database_name: String::new(),
                    schema_name,
                    table_name,
                    constraint_name,
                    constraint_type: Self::get_with_null(&row, "constraint_type", ColType::Char)
                        .unwrap(),
                    definition: Self::get_str_with_null(&row, "constraint_definition").unwrap(),
                },
            );
        }
        Ok(result)
    }

    pub async fn get_index(&mut self) -> Result<HashMap<String, StructModel>, Error> {
        let sql = format!("SELECT schemaname,tablename,indexdef, COALESCE(tablespace, 'pg_default') as tablespace, indexname 
            FROM pg_indexes 
            WHERE schemaname = '{}'", self.db);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut result = HashMap::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name): (String, String) = (
                PgStructExtractor::get_str_with_null(&row, "schemaname").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "tablename").unwrap(),
            );

            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }

            if self.filter.filter_tb(&schema_name, &table_name) {
                continue;
            }

            let index_name = Self::get_str_with_null(&row, "indexname").unwrap();
            let full_index_name = format!("{}.{}.{}", schema_name, table_name, index_name);
            result.insert(
                full_index_name,
                StructModel::IndexModel {
                    database_name: String::new(),
                    schema_name,
                    table_name,
                    index_name,
                    index_kind: IndexKind::Index,
                    index_type: String::new(),
                    comment: String::new(),
                    tablespace: Self::get_str_with_null(&row, "tablespace").unwrap(),
                    definition: Self::get_str_with_null(&row, "indexdef").unwrap(),
                    columns: Vec::new(),
                },
            );
        }
        Ok(result)
    }

    pub async fn get_comment(&mut self) -> Result<HashMap<String, StructModel>, Error> {
        let mut result = HashMap::new();

        // table comment
        let sql = format!("SELECT n.nspname, c.relname, d.description FROM pg_class c LEFT JOIN pg_namespace n on n.oid = c.relnamespace
            LEFT JOIN pg_description d ON c.oid = d.objoid AND d.objsubid = 0
            WHERE n.nspname ='{}' 
            AND d.description IS NOT null", self.db);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name): (String, String) = (
                PgStructExtractor::get_str_with_null(&row, "nspname").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "relname").unwrap(),
            );

            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }

            if self.filter.filter_tb(&schema_name, &table_name) {
                continue;
            }

            let schema_table_name = format!("{}.{}", schema_name, table_name);
            result.insert(
                schema_table_name,
                StructModel::CommentModel {
                    database_name: String::new(),
                    schema_name,
                    table_name,
                    column_name: String::new(),
                    comment: Self::get_str_with_null(&row, "description").unwrap(),
                },
            );
        }

        // column comment
        let sql = format!("SELECT n.nspname,c.relname, col_description(a.attrelid, a.attnum)as comment,format_type(a.atttypid, a.atttypmod)as type,a.attname as name,a.attnotnull as notnull
            FROM pg_class c LEFT JOIN pg_attribute a on a.attrelid =c.oid
            LEFT JOIN pg_namespace n on n.oid = c.relnamespace
            WHERE n.nspname ='{}' 
            AND a.attnum >0 and col_description(a.attrelid, a.attnum) is not null", self.db);

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name): (String, String) = (
                PgStructExtractor::get_str_with_null(&row, "nspname").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "relname").unwrap(),
            );

            if self.filter.filter_tb(&schema_name, &table_name) {
                continue;
            }

            let column_name = Self::get_str_with_null(&row, "name").unwrap();
            let full_comment_name = format!("{}.{}.{}", schema_name, table_name, column_name);
            result.insert(
                full_comment_name,
                StructModel::CommentModel {
                    database_name: String::new(),
                    schema_name,
                    table_name,
                    column_name,
                    comment: Self::get_str_with_null(&row, "comment").unwrap(),
                },
            );
        }

        Ok(result)
    }

    fn get_str_with_null(row: &PgRow, col_name: &str) -> Result<String, Error> {
        Self::get_with_null(row, col_name, ColType::Text)
    }

    fn get_with_null(row: &PgRow, col_name: &str, col_type: ColType) -> Result<String, Error> {
        let mut str_val = String::new();
        match col_type {
            ColType::Text => {
                let str_val_option: Option<String> = row.get(col_name);
                match str_val_option {
                    Some(s) => str_val = s,
                    None => {}
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
        match is_identity {
            Some(i) => {
                if i.to_lowercase() == "yes" && identity_generation.is_some() {
                    return identity_generation;
                }
            }
            None => {}
        }
        None
    }

    fn get_seq_name_by_default_value(default_value: String) -> Option<String> {
        // default_value such as:
        //   nextval('table_test_name_seq'::regclass)
        //   nextval('"table_test_name_seq"')
        //   nextval('struct_it.full_column_type_id_seq'::regclass)
        if default_value.is_empty() || !default_value.starts_with("nextval(") {
            return None;
        }
        let arr_tmp: Vec<&str> = default_value.split("'").collect();
        if arr_tmp.len() != 3 {
            println!(
                "default_value:[{}] is like a sequence used, but not valid in process.",
                default_value
            );
            return None;
        }
        let mut seq_name = arr_tmp[1];
        if seq_name.contains(".") {
            let real_name_start_index = seq_name.find(".").unwrap() + 1;
            seq_name = &seq_name[real_name_start_index..seq_name.len()];
        }
        if seq_name.starts_with("\"") && seq_name.ends_with("\"") {
            let (start_index, end_index) = (
                seq_name.find("\"").unwrap() + 1,
                seq_name.rfind("\"").unwrap(),
            );
            seq_name = &seq_name[start_index..end_index];
        }
        Some(seq_name.to_string())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn get_seq_name_by_default_value_test() {
        let mut opt: Option<String>;
        opt = PgStructExtractor::get_seq_name_by_default_value(String::from(
            "nextval('table_test_name_seq'::regclass)",
        ));
        assert_eq!(opt.unwrap(), String::from("table_test_name_seq"));

        opt = PgStructExtractor::get_seq_name_by_default_value(String::from(
            "nextval('table_test_name_seq')",
        ));
        assert_eq!(opt.unwrap(), String::from("table_test_name_seq"));

        opt = PgStructExtractor::get_seq_name_by_default_value(String::from(
            "nextval('\"table::123&^%@-_test_name_seq\"'::regclass)",
        ));
        assert_eq!(opt.unwrap(), String::from("table::123&^%@-_test_name_seq"));

        opt = PgStructExtractor::get_seq_name_by_default_value(String::from(
            "nextval('\"has_special_'\"'::regclass)",
        ));
        assert!(opt.is_none());

        opt = PgStructExtractor::get_seq_name_by_default_value(String::from(
            "nextval('schema.table_test_name_seq'::regclass)",
        ));
        assert_eq!(opt.unwrap(), String::from("table_test_name_seq"));

        opt = PgStructExtractor::get_seq_name_by_default_value(String::from(
            "nextval('\"has_special_schema_^&@\".\"table::123&^%@-_test_name_seq\"'::regclass)",
        ));
        assert_eq!(opt.unwrap(), String::from("table::123&^%@-_test_name_seq"));
    }
}
