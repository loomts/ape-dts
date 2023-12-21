#[derive(Debug, Clone, Default)]
pub struct Database {
    pub name: String,
    pub default_character_set_name: String,
    pub default_collation_name: String,
}
