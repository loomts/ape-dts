use strum::{Display, EnumString, IntoStaticStr};

#[derive(EnumString, IntoStaticStr, Display, PartialEq, Eq, Hash, Clone)]
pub enum CounterType {
    // time window counter, aggregate by: sum by window
    #[strum(serialize = "batch_write_failures")]
    BatchWriteFailures,
    #[strum(serialize = "serial_writes")]
    SerialWrites,

    // time window counter, aggregate by: avg by window
    #[strum(serialize = "rps")]
    Records,

    // time window counter, aggregate by: avg by count
    #[strum(serialize = "bytes_per_query")]
    BytesPerQuery,
    #[strum(serialize = "records_per_query")]
    RecordsPerQuery,
    #[strum(serialize = "rt_per_query")]
    RtPerQuery,
    #[strum(serialize = "buffer_size")]
    BufferSize,
    #[strum(serialize = "record_size")]
    RecordSize,

    // no window counter
    #[strum(serialize = "sinked_count")]
    SinkedCount,
}

pub enum AggreateType {
    Sum,
    AvgByWindow,
    AvgByCount,
}

pub enum WindowType {
    NoWindow,
    TimeWindow,
}

impl CounterType {
    pub fn get_window_type(&self) -> WindowType {
        match self {
            Self::BatchWriteFailures
            | Self::SerialWrites
            | Self::Records
            | Self::BytesPerQuery
            | Self::RecordsPerQuery
            | Self::RtPerQuery
            | Self::BufferSize
            | Self::RecordSize => WindowType::TimeWindow,
            Self::SinkedCount => WindowType::NoWindow,
        }
    }

    pub fn get_aggregate_type(&self) -> AggreateType {
        match self {
            Self::BatchWriteFailures | Self::SerialWrites | Self::SinkedCount => AggreateType::Sum,
            Self::BytesPerQuery
            | Self::RecordsPerQuery
            | Self::RtPerQuery
            | Self::BufferSize
            | Self::RecordSize => AggreateType::AvgByCount,
            Self::Records => AggreateType::AvgByWindow,
        }
    }
}
