use std::fmt;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Error {
    Unexpected { error: String },

    PreCheckError { error: String },

    IoError { error: String },

    EnvVarError { error: String },

    SqlxError { error: String },
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IoError {
            error: err.to_string(),
        }
    }
}

impl From<std::env::VarError> for Error {
    fn from(err: std::env::VarError) -> Self {
        Self::EnvVarError {
            error: err.to_string(),
        }
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Self::SqlxError {
            error: err.to_string(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Unexpected { error }
            | Error::PreCheckError { error }
            | Error::IoError { error }
            | Error::EnvVarError { error }
            | Error::SqlxError { error } => {
                let msg: String = if !error.is_empty() {
                    error.clone()
                } else {
                    String::from("unknown error")
                };
                msg.fmt(f)
            }
        }
    }
}
