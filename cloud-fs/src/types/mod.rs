mod path;

use std::cmp::{Ord, Ordering};
use std::error::Error;
use std::fmt;
use std::io;

use bytes::Bytes;

use crate::backends::Backend;
pub use path::FsPath;

pub type Data = Bytes;

#[derive(Clone, Debug)]
pub enum FsErrorType {
    ParseError,
    HostNotSupported,
    InvalidPath,
    InvalidSettings,
    TestFailure,
    Other,
}

#[derive(Clone, Debug)]
pub struct FsError {
    error_type: FsErrorType,
    description: String,
}

impl FsError {
    pub fn new<S: AsRef<str>>(error_type: FsErrorType, description: S) -> FsError {
        FsError {
            error_type,
            description: description.as_ref().to_owned(),
        }
    }

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

pub type FsResult<R> = Result<R, FsError>;

#[derive(Clone, Debug)]
pub struct FsSettings {
    pub(crate) backend: Backend,
    pub(crate) path: FsPath,
}

impl FsSettings {
    pub fn new(backend: Backend, path: FsPath) -> FsSettings {
        FsSettings { backend, path }
    }

    pub fn backend(&self) -> &Backend {
        &self.backend
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct FsFile {
    pub(crate) path: FsPath,
    pub(crate) size: u64,
}

impl FsFile {
    pub fn path(&self) -> &FsPath {
        &self.path
    }

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
