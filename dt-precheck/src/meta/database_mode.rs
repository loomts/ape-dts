pub struct Database {
    pub database_name: String,
}

pub struct Schema {
    pub database_name: String,
    pub schema_name: String,
}

pub struct Table {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
}

pub struct Column {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
}

pub struct Constraint {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
    pub constraint_name: String,
    pub constraint_type: String,
}
