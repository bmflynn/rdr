#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("overflow")]
    IntOverflow(#[from] std::num::TryFromIntError),

    #[error("hdf5 error name={name}: {msg}")]
    Hdf5 { name: String, msg: String },

    #[error("hdf5 attribute error for name={name} value={val}: {msg}")]
    Hdf5Attr {
        name: String,
        val: String,
        msg: String,
    },

    #[error("generic error: {0}")]
    Generic(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
