#[derive(Debug, Clone)]
pub struct Constraint {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub constraint_name: String,
    pub constraint_type: String,
    pub definition: String,
}
