use crate::{config::config_enums::DbType, meta::ddl_type::DdlType, utils::sql_util::SqlUtil};

#[derive(Default)]
pub struct ParsedDdl {
    pub ddl_type: DdlType,
    pub db_type: DbType,
    pub schema: Option<String>,
    pub tb: Option<String>,
    pub if_not_exists: bool,
    pub if_exists: bool,
    pub unparsed_part: String,
}

impl ParsedDdl {
    pub fn to_sql(&self) -> String {
        let mut sql = match self.ddl_type {
            DdlType::CreateDatabase => {
                let mut sql = "CREATE DATABASE".to_string();
                sql = self.append_if_not_exists(&sql);
                self.append_identifier(&sql, self.schema.as_ref().unwrap(), true)
            }

            DdlType::DropDatabase => {
                let mut sql = "DROP DATABASE".to_string();
                sql = self.append_if_exists(&sql);
                self.append_identifier(&sql, self.schema.as_ref().unwrap(), true)
            }

            DdlType::AlterDatabase => {
                let sql = "ALTER DATABASE";
                self.append_identifier(sql, self.schema.as_ref().unwrap(), true)
            }

            DdlType::CreateSchema => {
                let mut sql = "CREATE SCHEMA".to_string();
                sql = self.append_if_not_exists(&sql);
                self.append_identifier(&sql, self.schema.as_ref().unwrap(), true)
            }

            DdlType::DropSchema => {
                let mut sql = "DROP SCHEMA".to_string();
                sql = self.append_if_exists(&sql);
                self.append_identifier(&sql, self.schema.as_ref().unwrap(), true)
            }

            DdlType::AlterSchema => {
                let sql = "ALTER SCHEMA";
                self.append_identifier(sql, self.schema.as_ref().unwrap(), true)
            }

            DdlType::CreateTable => {
                let mut sql = "CREATE TABLE".to_string();
                sql = self.append_if_not_exists(&sql);
                self.append_tb(&sql)
            }

            DdlType::DropTable => {
                let mut sql = "DROP TABLE".to_string();
                sql = self.append_if_exists(&sql);
                self.append_tb(&sql)
            }

            DdlType::TruncateTable => self.append_tb("TRUNCATE TABLE"),

            DdlType::AlterTable => self.append_tb("ALTER TABLE"),

            DdlType::RenameTable => self.append_tb("RENAME TABLE"),

            // DdlType::CreateIndex => "CREATE INDEX",
            _ => return String::new(),
        };

        if !self.unparsed_part.is_empty() {
            sql = format!("{} {}", sql, self.unparsed_part);
        }
        sql
    }

    fn append_if_not_exists(&self, sql: &str) -> String {
        if self.if_not_exists {
            return format!("{} IF NOT EXISTS", sql);
        }
        sql.to_string()
    }

    fn append_if_exists(&self, sql: &str) -> String {
        if self.if_exists {
            return format!("{} IF EXISTS", sql);
        }
        sql.to_string()
    }

    fn append_tb(&self, sql: &str) -> String {
        let tb = SqlUtil::escape_by_db_type(self.tb.as_ref().unwrap(), &self.db_type);
        if let Some(schema) = &self.schema {
            let schema = SqlUtil::escape_by_db_type(schema, &self.db_type);
            format!("{} {}.{}", sql, schema, tb)
        } else {
            format!("{} {}", sql, tb)
        }
    }

    fn append_identifier(&self, sql: &str, identifier: &str, with_white_space: bool) -> String {
        let escaped_identifier = SqlUtil::escape_by_db_type(identifier, &self.db_type);
        if with_white_space {
            format!("{} {}", sql, escaped_identifier)
        } else {
            format!("{}{}", sql, escaped_identifier)
        }
    }
}
