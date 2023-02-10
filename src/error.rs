#[derive(Debug)]
pub enum Error {
    BinlogError {
        error: mysql_binlog_connector_rust::binlog_error::BinlogError,
    },

    SqlxError {
        error: sqlx::Error,
    },

    MetadataError {
        error: String,
    },

    IoError {
        error: std::io::Error,
    },

    YamlError {
        error: serde_yaml::Error,
    },

    EnvVarError {
        error: std::env::VarError,
    },

    ColumnNotMatch,
}

impl From<mysql_binlog_connector_rust::binlog_error::BinlogError> for Error {
    fn from(err: mysql_binlog_connector_rust::binlog_error::BinlogError) -> Self {
        Self::BinlogError { error: err }
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Self::SqlxError { error: err }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IoError { error: err }
    }
}

impl From<serde_yaml::Error> for Error {
    fn from(err: serde_yaml::Error) -> Self {
        Self::YamlError { error: err }
    }
}

impl From<std::env::VarError> for Error {
    fn from(err: std::env::VarError) -> Self {
        Self::EnvVarError { error: err }
    }
}
