use std::{num::TryFromIntError, str::Utf8Error};

use ccsds::spacepacket::PrimaryHeader;

/// An error occurred creating or manipulating an [Rdr](crate::Rdr).
#[derive(thiserror::Error, Debug)]
pub enum RdrError {
    #[error("Invalid IET microseconds time value {0}")]
    InvalidTime(u64),
    #[error("Granule start is less than spacecraft base time: {0}")]
    InvalidGranuleStart(u64),
    #[error("Invalid packet {0:?}")]
    InvalidPacket(PrimaryHeader),

    #[error("Failed to convert integer")]
    IntError(#[from] TryFromIntError),

    #[error("Invalid value")]
    Invalid(String),
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
    Io(#[from] std::io::Error),

    #[error("Config invalid: {0}")]
    ConfigInvalid(String),
    #[error("Failed to load config: {}", .source)]
    ConfigLoad {
        #[from]
        source: serde_yaml::Error,
    },
    #[error("No config for {0}")]
    ConfigNotFound(String),

    #[error(transparent)]
    RdrError(#[from] RdrError),

    #[error(transparent)]
    Hdf5(#[from] hdf5::Error),

    #[error("{0}")]
    Hdf5Other(String),

    #[error("hdf5-c erorr: {0}")]
    Hdf5Sys(String),
}

pub type Result<T> = std::result::Result<T, Error>;
