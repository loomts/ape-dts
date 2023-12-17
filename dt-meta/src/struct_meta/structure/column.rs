#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub column_name: String,
    pub order_position: u32,
    pub default_value: Option<String>,
    pub is_nullable: String,
    pub column_type: String, // varchar(100)
    pub column_key: String,  // PRI, MUL
    pub extra: String,       // auto_increment
    pub column_comment: String,
    pub generated: Option<String>,
    pub character_set: String,
    pub collation: String,
}
