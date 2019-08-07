use std::error;
use std::fmt;
use std::io;

use super::ObjectPath;

/// The kind of an [`StorageError`](struct.StorageError.html).
#[derive(Clone, Debug, PartialEq)]
pub enum StorageErrorKind {
    /// An error returned when attempting to access an invalid path.
    InvalidPath(ObjectPath),
    /// The object requested was not found.
    NotFound(ObjectPath),
    /// The operation was cancelled.
    Cancelled,
    /// The connection to storage failed.
    ConnectionFailed,
    /// The connection to storage was closed.
    ConnectionClosed,
    /// The service returned some invalid data.
    InvalidData,
    /// The credentials supplied were denied access.
    AccessDenied,
    /// An error returned if the configuration for a backend was invalid
    /// somehow.
    InvalidSettings,
    /// An internal failure, please report a bug!
    InternalError,
    /// Any other type of error (normally will have an inner error).
    Other,
}

/// Errors hit while interacting with storage backends. Generally wrapped by an
/// `io::Error`. Can be reached with `TryFrom`.
#[derive(Debug)]
pub struct StorageError {
    kind: StorageErrorKind,
    detail: String,
    inner: Option<Box<dyn error::Error + Send + Sync>>,
}

impl StorageError {
    /// Creates a new `StorageError`.
    pub fn new(kind: StorageErrorKind, detail: &str) -> StorageError {
        StorageError {
            kind,
            detail: detail.to_owned(),
            inner: None,
        }
    }

    /// Creates a new `StorageError` wrapping an inner error.
    pub fn from_inner<E>(kind: StorageErrorKind, detail: &str, inner: E) -> StorageError
    where
        E: 'static + error::Error + Send + Sync,
    {
        StorageError {
            kind,
            detail: detail.to_owned(),
            inner: Some(Box::new(inner)),
        }
    }

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
            StorageErrorKind::Cancelled => {
                write!(f, "The operation was cancelled: {}", &self.detail)
            }
            StorageErrorKind::ConnectionFailed => {
                write!(f, "The storage connection failed: {}", &self.detail)
            }
            StorageErrorKind::ConnectionClosed => {
                write!(f, "The storage connection was closed: {}", &self.detail)
            }
            StorageErrorKind::Other => write!(f, "An unknown error ocurred: {}", &self.detail),
            StorageErrorKind::InternalError => {
                write!(f, "An internal error occurred: {}", &self.detail)
            }
            StorageErrorKind::AccessDenied => write!(f, "Access was denied: {}", &self.detail),
            StorageErrorKind::InvalidSettings => write!(
                f,
                "Some of the settings passed were invalid: {}",
                &self.detail
            ),
        }
    }
}

impl From<StorageError> for io::Error {
    fn from(error: StorageError) -> io::Error {
        let kind = match error.kind() {
            StorageErrorKind::InvalidPath(_) => io::ErrorKind::InvalidData,
            StorageErrorKind::NotFound(_) => io::ErrorKind::NotFound,
            StorageErrorKind::InvalidData => io::ErrorKind::InvalidData,
            StorageErrorKind::InvalidSettings => io::ErrorKind::InvalidInput,
            StorageErrorKind::Cancelled => io::ErrorKind::ConnectionAborted,
            StorageErrorKind::ConnectionFailed => io::ErrorKind::ConnectionRefused,
            StorageErrorKind::ConnectionClosed => io::ErrorKind::NotConnected,
            StorageErrorKind::InternalError => io::ErrorKind::Other,
            StorageErrorKind::Other => io::ErrorKind::Other,
            StorageErrorKind::AccessDenied => io::ErrorKind::PermissionDenied,
        };

        io::Error::new(kind, error)
    }
}

/// The result type used throughout this crate.
pub type StorageResult<O> = Result<O, StorageError>;

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

impl From<ObjectPathError> for StorageError {
    fn from(error: ObjectPathError) -> StorageError {
        invalid_data(&error.to_string(), Some(error))
    }
}

/// An error that occurs while copying a file.
#[derive(Debug)]
pub enum TransferError {
    /// An error that came from the source of the transfer.
    SourceError(StorageError),
    /// An error that occured when writing to the target.
    TargetError(StorageError),
}

pub fn parse_error(spec: &str, message: &str) -> ObjectPathError {
    ObjectPathError {
        spec: spec.to_owned(),
        message: message.to_owned(),
    }
}

pub fn not_found<E>(path: ObjectPath, error: Option<E>) -> StorageError
where
    E: 'static + error::Error + Send + Sync,
{
    StorageError {
        kind: StorageErrorKind::NotFound(path),
        detail: String::new(),
        inner: error.map(|e| Box::new(e) as _),
    }
}

pub fn access_denied<E>(detail: &str, error: Option<E>) -> StorageError
where
    E: 'static + error::Error + Send + Sync,
{
    StorageError {
        kind: StorageErrorKind::AccessDenied,
        detail: detail.to_owned(),
        inner: error.map(|e| Box::new(e) as _),
    }
}

pub fn invalid_settings<E>(detail: &str, error: Option<E>) -> StorageError
where
    E: 'static + error::Error + Send + Sync,
{
    StorageError {
        kind: StorageErrorKind::InvalidSettings,
        detail: detail.to_owned(),
        inner: error.map(|e| Box::new(e) as _),
    }
}

pub fn invalid_data<E>(detail: &str, error: Option<E>) -> StorageError
where
    E: 'static + error::Error + Send + Sync,
{
    StorageError {
        kind: StorageErrorKind::InvalidData,
        detail: detail.to_owned(),
        inner: error.map(|e| Box::new(e) as _),
    }
}

pub fn cancelled<E>(detail: &str, error: Option<E>) -> StorageError
where
    E: 'static + error::Error + Send + Sync,
{
    StorageError {
        kind: StorageErrorKind::Cancelled,
        detail: detail.to_owned(),
        inner: error.map(|e| Box::new(e) as _),
    }
}

pub fn connection_failed<E>(detail: &str, error: Option<E>) -> StorageError
where
    E: 'static + error::Error + Send + Sync,
{
    StorageError {
        kind: StorageErrorKind::ConnectionFailed,
        detail: detail.to_owned(),
        inner: error.map(|e| Box::new(e) as _),
    }
}

pub fn connection_closed<E>(detail: &str, error: Option<E>) -> StorageError
where
    E: 'static + error::Error + Send + Sync,
{
    StorageError {
        kind: StorageErrorKind::ConnectionClosed,
        detail: detail.to_owned(),
        inner: error.map(|e| Box::new(e) as _),
    }
}

pub fn internal_error<E>(detail: &str, error: Option<E>) -> StorageError
where
    E: 'static + error::Error + Send + Sync,
{
    StorageError {
        kind: StorageErrorKind::InternalError,
        detail: detail.to_owned(),
        inner: error.map(|e| Box::new(e) as _),
    }
}

pub fn other_error<E>(detail: &str, error: Option<E>) -> StorageError
where
    E: 'static + error::Error + Send + Sync,
{
    StorageError {
        kind: StorageErrorKind::Other,
        detail: detail.to_owned(),
        inner: error.map(|e| Box::new(e) as _),
    }
}
