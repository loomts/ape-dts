#[derive(Debug, Clone)]
pub enum PositionInfo {
    MysqlCdc {
        binlog_filename: String,
        next_event_position: u64,
    },

    MysqlSnapshot {
        order_col_value: String,
        processed_count: u64,
        all_count: u64,
    },
}
