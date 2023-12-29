use strum::{Display, EnumString, IntoStaticStr};

#[derive(EnumString, IntoStaticStr, Display, PartialEq, Eq, Hash, Clone)]
pub enum CounterType {
    // time window counter, aggregate by: sum by window
    #[strum(serialize = "batch_write_failures")]
    BatchWriteFailures,
    #[strum(serialize = "serial_writes")]
    SerialWrites,

    // time window counter, aggregate by: avg by window
    #[strum(serialize = "record_count")]
    RecordCount,

    // time window counter, aggregate by: avg by count
    #[strum(serialize = "bytes_per_query")]
    BytesPerQuery,
    #[strum(serialize = "records_per_query")]
    RecordsPerQuery,
    #[strum(serialize = "rt_per_query")]
    RtPerQuery,
    #[strum(serialize = "buffer_size")]
    BufferSize,
    #[strum(serialize = "data_bytes")]
    DataBytes,
    #[strum(serialize = "record_size")]
    RecordSize,

    // no window counter
    #[strum(serialize = "sinked_count")]
    SinkedCount,
}

#[derive(EnumString, IntoStaticStr, Display, PartialEq, Eq, Hash, Clone)]
pub enum AggregateType {
    #[strum(serialize = "latest")]
    Latest,
    #[strum(serialize = "avg_by_sec")]
    AvgBySec,
    #[strum(serialize = "avg")]
    AvgByCount,
    #[strum(serialize = "max_by_sec")]
    MaxBySec,
    #[strum(serialize = "max")]
    MaxByCount,
    #[strum(serialize = "sum")]
    Sum,
    #[strum(serialize = "count")]
    Count,
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
            | Self::RecordCount
            | Self::BytesPerQuery
            | Self::RecordsPerQuery
            | Self::RtPerQuery
            | Self::BufferSize
            | Self::DataBytes
            | Self::RecordSize => WindowType::TimeWindow,
            Self::SinkedCount => WindowType::NoWindow,
        }
    }

    pub fn get_aggregate_types(&self) -> Vec<AggregateType> {
        match self.get_window_type() {
            WindowType::NoWindow => vec![AggregateType::Latest],

            WindowType::TimeWindow => match self {
                Self::BytesPerQuery
                | Self::RecordsPerQuery
                | Self::RtPerQuery
                | Self::BufferSize => {
                    vec![
                        AggregateType::AvgByCount,
                        AggregateType::Sum,
                        AggregateType::MaxByCount,
                    ]
                }

                Self::RecordSize => {
                    vec![AggregateType::AvgByCount]
                }

                Self::BatchWriteFailures
                | Self::SerialWrites
                | Self::RecordCount
                | Self::DataBytes => {
                    vec![
                        AggregateType::AvgBySec,
                        AggregateType::Sum,
                        AggregateType::MaxBySec,
                    ]
                }

                _ => vec![],
            },
        }
    }
}
