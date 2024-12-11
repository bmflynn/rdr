use std::str::Utf8Error;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed")]
    Failed,

    #[error("Not enough bytes creating {0}")]
    NotEnoughBytes(&'static str),

    #[error("transparent")]
    Utf8Error(#[from] Utf8Error),

    #[error(transparent)]
    Hdf5(#[from] hdf5::Error),
    #[error("Hdf5 c-api error: {0}")]
    Hdf5C(String),
    #[error(transparent)]
    Hdf5String(#[from] hdf5::types::StringError),
    #[error("hdf5 error: {0}")]
    Hdf5Other(&'static str),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Config invalid: {0}")]
    ConfigInvalid(String),
    #[error("Failed to load config: {}", .source)]
    ConfigLoad {
        #[from]
        source: serde_yaml::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
