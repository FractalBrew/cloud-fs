//! Contains the different storage backend implementations.
#[cfg(feature = "b2")]
mod b2;
#[cfg(feature = "file")]
mod file;

use crate::{ConnectFuture, FsImpl, FsSettings};

#[cfg(feature = "b2")]
pub use b2::B2Backend;
#[cfg(feature = "file")]
pub use file::FileBackend;

/// An enumeration of the available backends.
#[derive(Clone, Debug)]
pub enum Backend {
    #[cfg(feature = "file")]
    /// The (file backend)[file/index.html].
    File,
    #[cfg(feature = "b2")]
    /// The (B2 backend)[b2/index.html].
    B2,
}

/// Holds a backend implementation.
#[derive(Debug)]
pub enum BackendImplementation {
    #[cfg(feature = "file")]
    /// The (file backend)[struct.FileBackend.html].
    File(FileBackend),
    #[cfg(feature = "b2")]
    /// The (B2 backend)[struct.B2Backend.html].
    B2(B2Backend),
}

impl BackendImplementation {
    pub(crate) fn get(&self) -> Box<&FsImpl> {
        match self {
            #[cfg(feature = "file")]
            BackendImplementation::File(ref fs) => Box::new(fs),
            #[cfg(feature = "b2")]
            BackendImplementation::B2(ref fs) => Box::new(fs),
        }
    }
}

pub(crate) fn connect(settings: FsSettings) -> ConnectFuture {
    match settings.backend() {
        #[cfg(feature = "file")]
        Backend::File => FileBackend::connect(settings),
        #[cfg(feature = "b2")]
        Backend::B2 => B2Backend::connect(settings),
    }
}
