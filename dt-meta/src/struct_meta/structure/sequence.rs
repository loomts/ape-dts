#[derive(Debug, Clone)]
pub struct Sequence {
    pub sequence_name: String,
    pub database_name: String,
    pub schema_name: String,
    pub data_type: String,
    pub start_value: String,
    pub increment: String,
    pub minimum_value: String,
    pub maximum_value: String,
    pub cycle_option: String,
}
