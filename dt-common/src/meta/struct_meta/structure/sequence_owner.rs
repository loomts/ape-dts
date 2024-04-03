#[derive(Debug, Clone)]
pub struct SequenceOwner {
    pub sequence_name: String,
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
}
