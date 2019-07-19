//! Contains the different storage backend implementations.
#[cfg(feature = "file")]
mod file;

use std::fmt;

use crate::FsImpl;
#[cfg(feature = "file")]
pub use file::FileBackend;

/// An enumeration of the available backends.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Backend {
    #[cfg(feature = "file")]
    /// The (file backend)[file/index.html]. Included with the "file" feature.
    File,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            #[cfg(feature = "file")]
            Backend::File => f.write_str("file"),
        }
    }
}

/// Holds a backend implementation.
#[derive(Debug)]
pub(crate) enum BackendImplementation {
    #[cfg(feature = "file")]
    /// The (file backend)[struct.FileBackend.html].
    File(FileBackend),
}

impl BackendImplementation {
    pub(crate) fn get(&self) -> Box<&dyn FsImpl> {
        match self {
            #[cfg(feature = "file")]
            BackendImplementation::File(ref fs) => Box::new(fs),
        }
    }
}
