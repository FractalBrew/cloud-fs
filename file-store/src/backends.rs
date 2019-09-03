//! Contains the different storage backend implementations.
//!
//! Each backend allows for accessing files in a different storage system.
//! Normally you just crate a [`FileStore`](../enum.FileStore.html) from the
//! backend and then everything else is done by calls to the `FileStore` which
//! generally behave the same regardless of the backend.
#[cfg(feature = "b2")]
pub mod b2;
#[cfg(feature = "file")]
pub mod file;

use std::fmt;

/// An enumeration of the available backends.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Backend {
    #[cfg(feature = "file")]
    /// The [file backend](file/index.html). Included with the "file" feature.
    File,
    #[cfg(feature = "b2")]
    /// The [b2 backend](b2/index.html). Included with the "b2" feature.
    B2,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            #[cfg(feature = "file")]
            Backend::File => f.pad("file"),
            #[cfg(feature = "b2")]
            Backend::B2 => f.pad("b2"),
        }
    }
}
