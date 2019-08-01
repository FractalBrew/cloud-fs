use std::convert::TryFrom;
use std::error;
use std::fmt;
use std::io;

use super::StoragePath;

/// The kind of an [`StorageError`](struct.StorageError.html).
#[derive(Clone, Debug, PartialEq)]
pub enum StorageErrorKind {
    /// An error returned when attempting to access an invalid path.
    InvalidPath(StoragePath),
    /// The object requested was not found.
    NotFound(StoragePath),
    /// The service returned some invalid data.
    InvalidData,
    /// An error returned if the configuration for a backend was invalid
    /// somehow.
    InvalidSettings,
}

/// Errors hit while interacting with storage backends. Generally wrapped by an
/// `io::Error`. Can be reached with `TryFrom`.
#[derive(Clone, Debug)]
pub struct StorageError {
    kind: StorageErrorKind,
    detail: String,
}

impl StorageError {
    /// Returns the storage error kind.
    pub fn kind(&self) -> StorageErrorKind {
        self.kind.clone()
    }
}

impl error::Error for StorageError {}

macro_rules! write {
    ($f:expr, $($info:tt)*) => {
        $f.pad(&format!($($info)*))
    };
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            StorageErrorKind::InvalidPath(p) => write!(f, "The path '{}' was invalid", p),
            StorageErrorKind::NotFound(p) => write!(f, "The path '{}' was not found", p),
            StorageErrorKind::InvalidData => write!(f, "Invalid data: {}", &self.detail),
            StorageErrorKind::InvalidSettings => write!(
                f,
                "Some of the settings passed were invalid: {}",
                &self.detail
            ),
        }
    }
}

impl TryFrom<io::Error> for StorageError {
    type Error = ();

    fn try_from(error: io::Error) -> Result<StorageError, ()> {
        match error.into_inner() {
            Some(e) => match e.downcast_ref::<StorageError>() {
                Some(se) => Ok(se.clone()),
                None => Err(()),
            },
            None => Err(()),
        }
    }
}

impl TryFrom<io::Error> for StorageErrorKind {
    type Error = ();

    fn try_from(error: io::Error) -> Result<StorageErrorKind, ()> {
        match error.into_inner() {
            Some(e) => match e.downcast_ref::<StorageError>() {
                Some(se) => Ok(se.kind()),
                None => Err(()),
            },
            None => Err(()),
        }
    }
}

/// An error encountered when parsing or manipulating an [`ObjectPath`](../struct.ObjectPath.html).
#[derive(Clone, Debug)]
pub struct ObjectPathError {
    spec: String,
    message: String,
}

impl error::Error for ObjectPathError {}

impl fmt::Display for ObjectPathError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&format!(
            "Failed to parse '{}'. {}",
            &self.spec, &self.message
        ))
    }
}

impl From<ObjectPathError> for io::Error {
    fn from(error: ObjectPathError) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, error)
    }
}

/// An error that occurs while copying a file.
#[derive(Debug)]
pub enum TransferError {
    /// An error that came from the source of the transfer.
    SourceError(io::Error),
    /// An error that occured when writing to the target.
    TargetError(io::Error),
}

pub fn parse_error(spec: &str, message: &str) -> ObjectPathError {
    ObjectPathError {
        spec: spec.to_owned(),
        message: message.to_owned(),
    }
}

pub fn invalid_path(path: StoragePath, detail: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        StorageError {
            kind: StorageErrorKind::InvalidPath(path),
            detail: detail.to_owned(),
        },
    )
}

pub fn not_found(path: StoragePath) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        StorageError {
            kind: StorageErrorKind::NotFound(path),
            detail: String::new(),
        },
    )
}

pub fn invalid_settings(detail: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        StorageError {
            kind: StorageErrorKind::InvalidSettings,
            detail: detail.to_owned(),
        },
    )
}

pub fn invalid_data(detail: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        StorageError {
            kind: StorageErrorKind::InvalidData,
            detail: detail.to_owned(),
        },
    )
}
