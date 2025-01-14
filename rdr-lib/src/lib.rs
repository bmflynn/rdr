//! RDR mangling library.
//!
//! # Reference
//! Joint Polar Satellite System (JPSS) Common Data Format Control Book – External (CDFCB-X) Volume II - RDR Formats
//!
//! Unfortunately, the document does not seem to be publicly available from an official source,
//! but if you may have some luck if you search for CDFCB-X.
//!
mod collector;
mod error;
mod merge;
mod rdr;
mod time;
mod writer;

pub mod config;

pub use collector::*;
pub use error::*;
pub use merge::*;
pub use rdr::*;
pub use time::*;
pub use writer::*;
