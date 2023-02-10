#[derive(Debug, Clone)]
pub enum RowType {
    Insert,
    Update,
    Delete,
}

impl RowType {
    pub fn to_str(&self) -> &str {
        match self {
            RowType::Insert => "insert",
            RowType::Update => "update",
            RowType::Delete => "delete",
        }
    }
}
