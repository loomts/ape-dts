#[derive(Debug)]
pub enum Error {

    Unexpected {
        error: String,
    },

    MetadataError {
        error: String,
    },

    IoError {
        error: std::io::Error,
    },

    EnvVarError {
        error: std::env::VarError,
    },

    ColumnNotMatch,
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IoError { error: err }
    }
}

impl From<std::env::VarError> for Error {
    fn from(err: std::env::VarError) -> Self {
        Self::EnvVarError { error: err }
    }
}
