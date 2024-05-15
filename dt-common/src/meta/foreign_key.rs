use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ForeignKey {
    pub col: String,
    pub ref_schema: String,
    pub ref_tb: String,
    pub ref_col: String,
}
