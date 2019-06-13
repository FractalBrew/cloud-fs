mod path;

use std::cmp::{Ord, Ordering};
use std::error::Error;
use std::fmt;
use std::io;

use bytes::Bytes;

use crate::backends::Backend;
pub use path::FsPath;

pub type Data = Bytes;

/// The type of an [`FsError`](struct.FsError.html).
#[derive(Clone, Debug)]
pub enum FsErrorType {
    /// An error that occuring while parsing or manipulating a
    /// [`FsPath`](struct.FsPath.html].
    ParseError,
    /// An error a backend may return if an invalid storage host was requested.
    HostNotSupported,
    /// An error returns when attempting to access an invalid path.
    InvalidPath,
    /// An error returns if the [`FsSettings`](struct.FsSettings.html) is
    /// invalid in some way.
    InvalidSettings,
    /// An error used internally to mark a test failure.
    TestFailure,
    /// An unknown error type, usually a marker that this `FsError` was
    /// generated from a different error type.
    Other,
}

/// The main error type used throughout this crate.
#[derive(Clone, Debug)]
pub struct FsError {
    error_type: FsErrorType,
    description: String,
}

impl FsError {
    /// Creates a new `FsError` instance
    pub fn new<S: AsRef<str>>(error_type: FsErrorType, description: S) -> FsError {
        FsError {
            error_type,
            description: description.as_ref().to_owned(),
        }
    }

    /// Creates a new `FsError` out of some other kind of `Error`.
    pub fn from_error<E>(error: E) -> FsError
    where
        E: Error + fmt::Display,
    {
        Self::new(FsErrorType::Other, format!("{}", error))
    }
}

impl fmt::Display for FsError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(&self.description)
    }
}

impl Error for FsError {}

impl From<io::Error> for FsError {
    fn from(e: io::Error) -> FsError {
        FsError::from_error(e)
    }
}

/// A simple alias for a `Result` where the error is an [`FsError`](struct.FsError.html).
pub type FsResult<R> = Result<R, FsError>;

/// Settings used to create an [`Fs`](struct.Fs.html) instance.
///
/// Different backends may interpret these settings in different ways. Check
/// the [`backends`](backends/index.html) for specific details.
#[derive(Clone, Debug)]
pub struct FsSettings {
    pub(crate) backend: Backend,
    pub(crate) path: FsPath,
}

impl FsSettings {
    /// Creates settins for a specific backend with a given [`FsPath`](struct.FsPath.html).
    pub fn new(backend: Backend, path: FsPath) -> FsSettings {
        FsSettings { backend, path }
    }

    /// Gets this setting's current [`Backend`](backends/enum.Backend.html).
    pub fn backend(&self) -> &Backend {
        &self.backend
    }
}

/// A file in storage.
#[derive(Clone, PartialEq, Debug)]
pub struct FsFile {
    pub(crate) path: FsPath,
    pub(crate) size: u64,
}

impl FsFile {
    /// Gets the file's path.
    pub fn path(&self) -> &FsPath {
        &self.path
    }

    /// Gets the file's size.
    pub fn size(&self) -> u64 {
        self.size
    }
}

impl Eq for FsFile {}

impl PartialOrd for FsFile {
    fn partial_cmp(&self, other: &FsFile) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FsFile {
    fn cmp(&self, other: &FsFile) -> Ordering {
        self.path.cmp(&other.path)
    }
}
