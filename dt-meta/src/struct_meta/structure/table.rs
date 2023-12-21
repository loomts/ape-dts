use super::column::Column;

#[derive(Debug, Clone, Default)]
pub struct Table {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub engine_name: String, // innodb
    pub table_comment: String,
    pub charset: String,
    pub collate: String,
    pub columns: Vec<Column>,
}
