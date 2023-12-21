#[derive(Debug, Clone, Default)]
pub struct Index {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub index_name: String,
    pub index_kind: IndexKind,
    pub index_type: String, // btree, hash
    pub comment: String,
    pub tablespace: String,
    pub definition: String,
    pub columns: Vec<IndexColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum IndexKind {
    PrimaryKey,
    Unique,
    Index,
    #[default]
    Unkown,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndexColumn {
    pub column_name: String,
    pub seq_in_index: u32,
}
