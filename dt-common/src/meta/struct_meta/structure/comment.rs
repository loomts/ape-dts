#[derive(Debug, Clone)]
pub struct Comment {
    pub comment_type: CommentType,
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
    pub comment: String,
}

#[derive(Clone, Debug)]
pub enum CommentType {
    Table,
    Column,
}
