#[derive(Clone, Debug, PartialEq, Default)]
pub struct Column {
    pub column_name: String,
    pub ordinal_position: u32,
    pub column_default: Option<String>,
    pub is_nullable: String,
    pub column_type: String, // varchar(100)
    pub column_key: String,  // PRI, MUL
    pub extra: String,       // auto_increment
    pub column_comment: String,
    pub generated: Option<String>,
    pub character_set_name: String,
    pub collation_name: String,
}
