use std::{num::TryFromIntError, str::Utf8Error};

use ccsds::spacepacket::PrimaryHeader;

/// An error occurred creating or manipulating an [Rdr](crate::Rdr).
#[derive(thiserror::Error, Debug)]
pub enum RdrError {
    #[error("Invalid IET microseconds time value {0}")]
    InvalidTime(u64),
    #[error("Invalid packet header {0:?}")]
    InvalidPktHeader(PrimaryHeader),
    #[error("Invalid packet apid for RDR product {0} {1}")]
    InvalidPktApid(String, u16),

    #[error("failed to convert integer")]
    IntError(#[from] TryFromIntError),
}

#[derive(thiserror::Error, Debug)]
pub enum H5Error {
    /// Error calling the hdf5-sys (ffi).
    #[error("{0}")]
    Sys(String),

    #[error(transparent)]
    Hdf5(#[from] hdf5::Error),

    #[error(transparent)]
    Hdf5String(#[from] hdf5::types::StringError),

    /// General error
    #[error("hdf5 error: {0}")]
    Other(String),
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed")]
    Failed,

    #[error("Not enough bytes creating {0}")]
    NotEnoughBytes(&'static str),

    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),

    #[error(transparent)]
    H5(#[from] H5Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Config invalid: {0}")]
    ConfigInvalid(String),
    #[error("Failed to load config: {}", .source)]
    ConfigLoad {
        #[from]
        source: serde_yaml::Error,
    },

    #[error(transparent)]
    RdrError(#[from] RdrError),
}

pub type Result<T> = std::result::Result<T, Error>;
