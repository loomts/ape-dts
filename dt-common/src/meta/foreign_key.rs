use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ForeignKey {
    pub schema: String,
    pub tb: String,
    pub col: String,
    pub ref_schema: String,
    pub ref_tb: String,
    pub ref_col: String,
}
