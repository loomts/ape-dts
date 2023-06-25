use std::collections::HashMap;

use dt_common::{error::Error, utils::rdb_filter::RdbFilter};
use dt_meta::struct_meta::{
    col_model::ColType,
    database_model::{Column, CommentType, IndexKind, StructModel},
};
use futures::TryStreamExt;
use sqlx::{postgres::PgRow, Pool, Postgres, Row};

pub struct PgStructFetcher {
    pub conn_pool: Pool<Postgres>,
    pub db: String,
    pub filter: Option<RdbFilter>,
}

impl PgStructFetcher {
    pub async fn get_sequence(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let mut models = HashMap::new();

        // query target seq
        let struct_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::SequenceModel {
                sequence_name: String::from(""),
                database_name: String::from(""),
                schema_name: self.db.clone(),
                data_type: String::from(""),
                start_value: String::from(""),
                increment: String::from(""),
                min_value: String::from(""),
                max_value: String::from(""),
                is_circle: String::from(""),
            },
        };
        let sql = self.sql_builder(&struct_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name, seq_name): (String, String, String) = (
                PgStructFetcher::get_str_with_null(&row, "sequence_schema").unwrap(),
                PgStructFetcher::get_str_with_null(&row, "table_name").unwrap(),
                PgStructFetcher::get_str_with_null(&row, "sequence_name").unwrap(),
            );

            if schema_name.is_empty() || table_name.is_empty() || seq_name.is_empty() {
                continue;
            }
            if let Some(filter) = &mut self.filter {
                if filter.filter_tb(&schema_name, &table_name) {
                    continue;
                }
            }

            let schema_seq_name = format!("{}.{}", schema_name, seq_name);
            models.insert(
                schema_seq_name,
                StructModel::SequenceModel {
                    sequence_name: seq_name,
                    database_name: Self::get_str_with_null(&row, "sequence_catalog").unwrap(),
                    schema_name,
                    data_type: Self::get_str_with_null(&row, "data_type").unwrap(),
                    start_value: row.get("start_value"),
                    increment: row.get("increment"),
                    min_value: row.get("minimum_value"),
                    max_value: row.get("maximum_value"),
                    is_circle: Self::get_str_with_null(&row, "cycle_option").unwrap(),
                },
            );
        }

        Ok(models)
    }

    pub async fn get_sequence_owner(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let mut seq_owners = HashMap::new();

        // query seq ownership, and put the StructModel into memory, will push to queue after get_tables
        let seq_owner_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::SequenceOwnerModel {
                sequence_name: String::from(""),
                database_name: String::from(""),
                schema_name: self.db.clone(),
                owner_table_name: String::from(""),
                owner_table_column_name: String::from(""),
            },
        };
        let sql = self.sql_builder(&seq_owner_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name, seq_name): (String, String, String) = (
                PgStructFetcher::get_str_with_null(&row, "nspname").unwrap(),
                PgStructFetcher::get_str_with_null(&row, "table_name").unwrap(),
                PgStructFetcher::get_str_with_null(&row, "relname").unwrap(),
            );

            if schema_name.is_empty() || table_name.is_empty() || seq_name.is_empty() {
                continue;
            }
            if let Some(filter) = &mut self.filter {
                if filter.filter_tb(&schema_name, &table_name) {
                    continue;
                }
            }

            let schema_seq_name = format!("{}.{}", schema_name, seq_name);

            seq_owners
                .entry(schema_seq_name)
                .or_insert(StructModel::SequenceOwnerModel {
                    sequence_name: seq_name,
                    database_name: String::new(),
                    schema_name,
                    owner_table_name: Self::get_str_with_null(&row, "table_name").unwrap(),
                    owner_table_column_name: Self::get_str_with_null(&row, "column_name").unwrap(),
                });
        }

        Ok(seq_owners)
    }

    pub async fn get_table(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let struct_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::TableModel {
                database_name: String::from(""),
                schema_name: self.db.clone(),
                table_name: String::from(""),
                engine_name: String::from(""),
                table_comment: String::from(""),
                columns: vec![],
            },
        };
        let sql = self.sql_builder(&struct_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut results = HashMap::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name) = (
                PgStructFetcher::get_str_with_null(&row, "table_schema").unwrap(),
                PgStructFetcher::get_str_with_null(&row, "table_name").unwrap(),
            );

            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }
            if let Some(filter) = &mut self.filter {
                if filter.filter_tb(&schema_name, &table_name) {
                    continue;
                }
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
                if let StructModel::TableModel { columns, .. } = model {
                    columns.push(column);
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

    pub async fn get_constraint(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let struct_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::ConstraintModel {
                database_name: String::from(""),
                schema_name: self.db.clone(),
                table_name: String::from(""),
                constraint_name: String::from(""),
                constraint_type: String::from(""),
                definition: String::from(""),
            },
        };
        let sql = self.sql_builder(&struct_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut result = HashMap::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name): (String, String) = (
                PgStructFetcher::get_str_with_null(&row, "nspname").unwrap(),
                PgStructFetcher::get_str_with_null(&row, "relname").unwrap(),
            );

            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }
            if let Some(filter) = &mut self.filter {
                if filter.filter_tb(&schema_name, &table_name) {
                    continue;
                }
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

    pub async fn get_index(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let struct_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::IndexModel {
                database_name: String::from(""),
                schema_name: self.db.clone(),
                table_name: String::from(""),
                index_name: String::from(""),
                index_kind: IndexKind::Unkown,
                index_type: String::from(""),
                comment: String::from(""),
                tablespace: String::from(""),
                definition: String::from(""),
                columns: vec![],
            },
        };
        let sql = self.sql_builder(&struct_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut result = HashMap::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name): (String, String) = (
                PgStructFetcher::get_str_with_null(&row, "schemaname").unwrap(),
                PgStructFetcher::get_str_with_null(&row, "tablename").unwrap(),
            );

            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }
            if let Some(filter) = &mut self.filter {
                if filter.filter_tb(&schema_name, &table_name) {
                    continue;
                }
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

    pub async fn get_table_comment(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let mut result = HashMap::new();

        // table comment
        let struct_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::CommentModel {
                comment_type: CommentType::Table,
                database_name: String::from(""),
                schema_name: self.db.clone(),
                table_name: String::from(""),
                column_name: String::from(""),
                comment: String::from(""),
            },
        };
        let sql = self.sql_builder(&struct_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name): (String, String) = (
                PgStructFetcher::get_str_with_null(&row, "nspname").unwrap(),
                PgStructFetcher::get_str_with_null(&row, "relname").unwrap(),
            );

            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }
            if let Some(filter) = &mut self.filter {
                if filter.filter_tb(&schema_name, &table_name) {
                    continue;
                }
            }

            let schema_table_name = format!("{}.{}", schema_name, table_name);
            result.insert(
                schema_table_name,
                StructModel::CommentModel {
                    comment_type: CommentType::Table,
                    database_name: String::new(),
                    schema_name,
                    table_name,
                    column_name: String::new(),
                    comment: Self::get_str_with_null(&row, "description").unwrap(),
                },
            );
        }

        Ok(result)
    }

    pub async fn get_column_comment(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let mut result = HashMap::new();

        // column comment
        let struct_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::CommentModel {
                comment_type: CommentType::Column,
                database_name: String::from(""),
                schema_name: self.db.clone(),
                table_name: String::from(""),
                column_name: String::from(""),
                comment: String::from(""),
            },
        };
        let sql = self.sql_builder(&struct_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        while let Some(row) = rows.try_next().await.unwrap() {
            let (schema_name, table_name): (String, String) = (
                PgStructFetcher::get_str_with_null(&row, "nspname").unwrap(),
                PgStructFetcher::get_str_with_null(&row, "relname").unwrap(),
            );

            if let Some(filter) = &mut self.filter {
                if filter.filter_tb(&schema_name, &table_name) {
                    continue;
                }
            }

            let column_name = Self::get_str_with_null(&row, "name").unwrap();
            let full_comment_name = format!("{}.{}.{}", schema_name, table_name, column_name);
            result.insert(
                full_comment_name,
                StructModel::CommentModel {
                    comment_type: CommentType::Column,
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

    pub async fn fetch_with_model(
        mut self,
        struct_model: &StructModel,
    ) -> Result<Option<StructModel>, Error> {
        let model_option = Some(struct_model.to_owned());
        let result = match struct_model {
            StructModel::TableModel { .. } => self.get_table(&model_option).await,
            StructModel::ConstraintModel { .. } => self.get_constraint(&model_option).await,
            StructModel::IndexModel { .. } => self.get_index(&model_option).await,
            StructModel::CommentModel { comment_type, .. } => match comment_type {
                CommentType::Table => self.get_table_comment(&model_option).await,
                CommentType::Column => self.get_column_comment(&model_option).await,
            },
            StructModel::SequenceModel { .. } => self.get_sequence(&model_option).await,
            StructModel::SequenceOwnerModel { .. } => self.get_sequence_owner(&model_option).await,
            _ => {
                let result: HashMap<String, StructModel> = HashMap::new();
                Ok(result)
            }
        };
        match result {
            Ok(r) => {
                let result_option = r.values().next().map(|f| f.to_owned());
                Ok(result_option)
            }
            Err(e) => Err(e),
        }
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

    fn sql_builder(&self, struct_model: &StructModel) -> String {
        let sql: String = match struct_model {
            StructModel::TableModel {
                database_name: _,
                schema_name,
                table_name,
                engine_name: _,
                table_comment: _,
                columns: _,
            } => {
                let mut s = format!("SELECT c.table_schema,c.table_name,c.column_name, c.data_type, c.udt_name, c.character_maximum_length, c.is_nullable, c.column_default, c.numeric_precision, c.numeric_scale, c.is_identity, c.identity_generation,c.ordinal_position 
FROM information_schema.columns c WHERE table_schema ='{}' ", schema_name);
                if !table_name.is_empty() {
                    s = format!("{} and table_name = '{}' ", s, table_name);
                }
                format!(
                    "{} ORDER BY table_schema, table_name, c.ordinal_position ",
                    s
                )
            }
            StructModel::ConstraintModel {
                database_name: _,
                schema_name,
                table_name: _,
                constraint_name,
                constraint_type: _,
                definition: _,
            } => {
                let mut s = format!("SELECT nsp.nspname, rel.relname, con.conname as constraint_name, con.contype as constraint_type,pg_get_constraintdef(con.oid) as constraint_definition
FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace WHERE nsp.nspname ='{}' ", schema_name);
                if !constraint_name.is_empty() {
                    s = format!("{} and con.conname = '{}' ", s, constraint_name);
                }
                format!("{} ORDER BY nsp.nspname,rel.relname ", s)
            }
            StructModel::IndexModel {
                database_name: _,
                schema_name,
                table_name: _,
                index_name,
                index_kind: _,
                index_type: _,
                comment: _,
                tablespace: _,
                definition: _,
                columns: _,
            } => {
                let mut s = format!("SELECT schemaname,tablename,indexdef, COALESCE(tablespace, 'pg_default') as tablespace, indexname 
FROM pg_indexes WHERE schemaname = '{}'", schema_name);
                if !index_name.is_empty() {
                    s = format!("{} and indexname= '{}' ", s, index_name);
                }
                format!(" {} order by schemaname, tablename, indexname ", s)
            }
            StructModel::CommentModel {
                comment_type,
                database_name: _,
                schema_name,
                table_name,
                column_name,
                comment: _,
            } => match comment_type {
                CommentType::Table => {
                    let mut s = format!("SELECT n.nspname, c.relname, d.description FROM pg_class c LEFT JOIN pg_namespace n on n.oid = c.relnamespace
LEFT JOIN pg_description d ON c.oid = d.objoid AND d.objsubid = 0 WHERE n.nspname ='{}' AND d.description IS NOT null", schema_name);
                    if !table_name.is_empty() {
                        s = format!("{} and c.relname = '{}' ", s, table_name);
                    }
                    s
                }
                CommentType::Column => {
                    let mut s = format!("SELECT n.nspname,c.relname, col_description(a.attrelid, a.attnum)as comment,format_type(a.atttypid, a.atttypmod)as type,a.attname as name,a.attnotnull as notnull
FROM pg_class c LEFT JOIN pg_attribute a on a.attrelid =c.oid LEFT JOIN pg_namespace n on n.oid = c.relnamespace WHERE n.nspname ='{}' 
AND a.attnum >0 and col_description(a.attrelid, a.attnum) is not null", schema_name);
                    if !table_name.is_empty() && !column_name.is_empty() {
                        s = format!(
                            "{} and c.relname = '{}' and a.attname = '{}' ",
                            s, table_name, column_name
                        );
                    }
                    s
                }
            },
            StructModel::SequenceModel {
                sequence_name,
                database_name: _,
                schema_name,
                data_type: _,
                start_value: _,
                increment: _,
                min_value: _,
                max_value: _,
                is_circle: _,
            } => {
                let mut s = format!(" SELECT obj.sequence_catalog,obj.sequence_schema,tab.relname as table_name, obj.sequence_name,obj.data_type,obj.start_value,obj.minimum_value,obj.maximum_value,obj.increment,obj.cycle_option 
FROM information_schema.sequences obj      
join pg_class as seq on (seq.relname = obj.sequence_name)
join pg_namespace ns on (seq.relnamespace = ns.oid) 
join pg_depend as dep on (seq.relfilenode = dep.objid) 
join pg_class as tab on (dep.refobjid = tab.relfilenode) 
 where ns.nspname='{}' and dep.deptype='a' ", schema_name);
                if !sequence_name.is_empty() {
                    s = format!("{} and obj.sequence_name = '{}' ", s, sequence_name);
                }
                s
            }
            StructModel::SequenceOwnerModel {
                sequence_name,
                database_name: _,
                schema_name,
                owner_table_name,
                owner_table_column_name,
            } => {
                let mut s = format!("select seq.relname,tab.relname as table_name, attr.attname as column_name, ns.nspname 
                from pg_class as seq 
                join pg_namespace ns on (seq.relnamespace = ns.oid) 
                join pg_depend as dep on (seq.relfilenode = dep.objid) 
                join pg_class as tab on (dep.refobjid = tab.relfilenode) 
                join pg_attribute as attr on (attr.attnum = dep.refobjsubid and attr.attrelid = dep.refobjid) 
                where dep.deptype='a' and seq.relkind='S' and ns.nspname = '{}' ", schema_name);
                if !sequence_name.is_empty() {
                    s = format!("{} and seq.relname = '{}' ", s, sequence_name);
                }
                if !owner_table_name.is_empty() {
                    s = format!("{} and tab.relname = '{}' ", s, owner_table_name);
                }
                if !owner_table_column_name.is_empty() {
                    s = format!("{} and attr.attname = '{}' ", s, owner_table_column_name);
                }
                s
            }
            _ => String::from(""),
        };
        sql
    }

    #[allow(dead_code)]
    #[deprecated]
    fn get_seq_name_by_default_value(default_value: String) -> Option<String> {
        // default_value such as:
        //   nextval('table_test_name_seq'::regclass)
        //   nextval('"table_test_name_seq"')
        //   nextval('struct_it.full_column_type_id_seq'::regclass)
        if default_value.is_empty() || !default_value.starts_with("nextval(") {
            return None;
        }
        let arr_tmp: Vec<&str> = default_value.split('\'').collect();
        if arr_tmp.len() != 3 {
            println!(
                "default_value:[{}] is like a sequence used, but not valid in process.",
                default_value
            );
            return None;
        }
        let mut seq_name = arr_tmp[1];
        if seq_name.contains('.') {
            let real_name_start_index = seq_name.find('.').unwrap() + 1;
            seq_name = &seq_name[real_name_start_index..seq_name.len()];
        }
        if seq_name.starts_with('\"') && seq_name.ends_with('\"') {
            let (start_index, end_index) = (
                seq_name.find('\"').unwrap() + 1,
                seq_name.rfind('\"').unwrap(),
            );
            seq_name = &seq_name[start_index..end_index];
        }
        Some(seq_name.to_string())
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use crate::meta_fetcher::pg::pg_struct_fetcher::PgStructFetcher;

    #[test]
    fn get_seq_name_by_default_value_test() {
        let mut opt: Option<String>;
        opt = PgStructFetcher::get_seq_name_by_default_value(String::from(
            "nextval('table_test_name_seq'::regclass)",
        ));
        assert_eq!(opt.unwrap(), String::from("table_test_name_seq"));

        opt = PgStructFetcher::get_seq_name_by_default_value(String::from(
            "nextval('table_test_name_seq')",
        ));
        assert_eq!(opt.unwrap(), String::from("table_test_name_seq"));

        opt = PgStructFetcher::get_seq_name_by_default_value(String::from(
            "nextval('\"table::123&^%@-_test_name_seq\"'::regclass)",
        ));
        assert_eq!(opt.unwrap(), String::from("table::123&^%@-_test_name_seq"));

        opt = PgStructFetcher::get_seq_name_by_default_value(String::from(
            "nextval('\"has_special_'\"'::regclass)",
        ));
        assert!(opt.is_none());

        opt = PgStructFetcher::get_seq_name_by_default_value(String::from(
            "nextval('schema.table_test_name_seq'::regclass)",
        ));
        assert_eq!(opt.unwrap(), String::from("table_test_name_seq"));

        opt = PgStructFetcher::get_seq_name_by_default_value(String::from(
            "nextval('\"has_special_schema_^&@\".\"table::123&^%@-_test_name_seq\"'::regclass)",
        ));
        assert_eq!(opt.unwrap(), String::from("table::123&^%@-_test_name_seq"));
    }
}
