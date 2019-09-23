// Copyright 2019 Dave Townsend
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::convert::Infallible;
use std::error;
use std::fmt;
use std::io;

use log::error;

use super::ObjectPath;

/// The kind of an [`StorageError`](struct.StorageError.html).
#[derive(Clone, Debug, PartialEq)]
pub enum StorageErrorKind {
    /// An error occurred while parsing an ObjectPath.
    ObjectPathParse(String),
    /// An error returned when attempting to access an invalid path.
    InvalidPath(ObjectPath),
    /// The object requested was not found.
    NotFound(ObjectPath),
    /// The object already exists.
    AlreadyExists(ObjectPath),
    /// The operation was cancelled.
    Cancelled,
    /// The connection to storage failed.
    ConnectionFailed,
    /// The connection to storage was closed.
    ConnectionClosed,
    /// The service experienced an unknown failure.
    ServiceError,
    /// The service returned some invalid data.
    InvalidData,
    /// The credentials supplied were denied access.
    AccessDenied,
    /// Access has expired. Reconnecting may solve the issue.
    AccessExpired,
    /// An error returned if the configuration for a backend was invalid
    /// somehow.
    InvalidSettings,
    /// Some kind of limit on use use of the service has been reached.
    OverQuota,
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
    detail: Option<String>,
}

impl StorageError {
    /// Creates a new `StorageError`.
    pub fn new(kind: StorageErrorKind, detail: Option<&str>) -> StorageError {
        StorageError {
            kind,
            detail: detail.map(ToOwned::to_owned),
        }
    }

    /// Returns the storage error kind.
    pub fn kind(&self) -> StorageErrorKind {
        self.kind.clone()
    }

    // fn write<A, B>(&self, f: &mut fmt::Formatter, with_detail: A, without_detail: B) -> fmt::Result
    // where
    //     A: AsRef<str>,
    //     B: AsRef<str>,
    // {
    //     match self.detail {
    //         Some(ref detail) => f.pad(&with_detail.as_ref().replace("{}", detail)),
    //         None => f.pad(without_detail.as_ref()),
    //     }
    // }

    fn default_write<A>(&self, f: &mut fmt::Formatter, message: A) -> fmt::Result
    where
        A: AsRef<str>,
    {
        match self.detail {
            Some(ref detail) => write!(f, "{}: {}.", message.as_ref(), detail),
            None => write!(f, "{}.", message.as_ref()),
        }
    }
}

impl error::Error for StorageError {}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            StorageErrorKind::ObjectPathParse(s) => {
                self.default_write(f, format!("Failed to parse '{}'", s))
            }
            StorageErrorKind::InvalidPath(p) => {
                self.default_write(f, format!("The path '{}' was invalid", p))
            }
            StorageErrorKind::NotFound(p) => {
                self.default_write(f, format!("The path '{}' was not found", p))
            }
            StorageErrorKind::AlreadyExists(p) => {
                self.default_write(f, format!("The path '{}' already exists", p))
            }
            StorageErrorKind::InvalidData => self.default_write(f, "Invalid data"),
            StorageErrorKind::Cancelled => self.default_write(f, "The operation was cancelled"),
            StorageErrorKind::ConnectionFailed => {
                self.default_write(f, "The storage connection failed")
            }
            StorageErrorKind::ConnectionClosed => {
                self.default_write(f, "The storage connection was closed")
            }
            StorageErrorKind::Other => self.default_write(f, "An unknown error ocurred"),
            StorageErrorKind::InternalError => self.default_write(f, "An internal error occurred"),
            StorageErrorKind::AccessDenied => self.default_write(f, "Access was denied"),
            StorageErrorKind::AccessExpired => self.default_write(f, "Access has expired"),
            StorageErrorKind::InvalidSettings => {
                self.default_write(f, "Some of the settings passed were invalid")
            }
            StorageErrorKind::OverQuota => {
                self.default_write(f, "A storage limit has been reached")
            }
            StorageErrorKind::ServiceError => {
                self.default_write(f, "The storage system encountered an error")
            }
        }
    }
}

impl From<StorageError> for io::Error {
    fn from(error: StorageError) -> io::Error {
        let kind = match error.kind() {
            StorageErrorKind::ObjectPathParse(_) => io::ErrorKind::InvalidData,
            StorageErrorKind::InvalidPath(_) => io::ErrorKind::InvalidData,
            StorageErrorKind::NotFound(_) => io::ErrorKind::NotFound,
            StorageErrorKind::AlreadyExists(_) => io::ErrorKind::AlreadyExists,
            StorageErrorKind::InvalidData => io::ErrorKind::InvalidData,
            StorageErrorKind::InvalidSettings => io::ErrorKind::InvalidInput,
            StorageErrorKind::Cancelled => io::ErrorKind::ConnectionAborted,
            StorageErrorKind::ConnectionFailed => io::ErrorKind::ConnectionRefused,
            StorageErrorKind::ConnectionClosed => io::ErrorKind::NotConnected,
            StorageErrorKind::InternalError => io::ErrorKind::Other,
            StorageErrorKind::Other => io::ErrorKind::Other,
            StorageErrorKind::AccessDenied => io::ErrorKind::PermissionDenied,
            StorageErrorKind::AccessExpired => io::ErrorKind::PermissionDenied,
            StorageErrorKind::ServiceError => io::ErrorKind::Other,
            StorageErrorKind::OverQuota => io::ErrorKind::Other,
        };

        io::Error::new(kind, error)
    }
}

impl From<io::Error> for StorageError {
    fn from(error: io::Error) -> StorageError {
        let kind = match error.kind() {
            io::ErrorKind::NotFound => StorageErrorKind::NotFound(ObjectPath::empty()),
            io::ErrorKind::AlreadyExists => StorageErrorKind::AlreadyExists(ObjectPath::empty()),
            io::ErrorKind::PermissionDenied => StorageErrorKind::AccessDenied,
            io::ErrorKind::ConnectionRefused => StorageErrorKind::ConnectionFailed,
            io::ErrorKind::ConnectionReset => StorageErrorKind::ConnectionClosed,
            io::ErrorKind::ConnectionAborted => StorageErrorKind::ConnectionFailed,
            io::ErrorKind::NotConnected => StorageErrorKind::ConnectionClosed,
            io::ErrorKind::BrokenPipe => StorageErrorKind::ConnectionClosed,
            io::ErrorKind::InvalidInput => StorageErrorKind::InvalidData,
            io::ErrorKind::InvalidData => StorageErrorKind::InvalidData,
            _ => StorageErrorKind::Other,
        };

        StorageError {
            kind,
            detail: Some(error.to_string()),
        }
    }
}

impl From<Infallible> for StorageError {
    fn from(_: Infallible) -> StorageError {
        unimplemented!();
    }
}

/// The result type used throughout this crate.
pub type StorageResult<O> = Result<O, StorageError>;

/// An error that occurs while copying a file.
#[derive(Debug)]
pub enum TransferError {
    /// An error that came from the source of the transfer.
    SourceError(StorageError),
    /// An error that occured when writing to the target.
    TargetError(StorageError),
}

pub fn parse_error(spec: &str, detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::ObjectPathParse(spec.to_owned()), detail)
}

pub fn invalid_path(path: ObjectPath, detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::InvalidPath(path), detail)
}

pub fn not_found(path: ObjectPath, detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::NotFound(path), detail)
}

pub fn already_exists(path: ObjectPath, detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::AlreadyExists(path), detail)
}

pub fn over_quota(detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::OverQuota, detail)
}

pub fn access_denied(detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::AccessDenied, detail)
}

pub fn access_expired(detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::AccessExpired, detail)
}

pub fn invalid_settings(detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::InvalidSettings, detail)
}

pub fn service_error(detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::ServiceError, detail)
}

pub fn invalid_data(detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::InvalidData, detail)
}

pub fn cancelled(detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::Cancelled, detail)
}

pub fn connection_failed(detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::ConnectionFailed, detail)
}

pub fn connection_closed(detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::ConnectionClosed, detail)
}

pub fn internal_error(detail: Option<&str>) -> StorageError {
    error!("An internal error occurred");
    StorageError::new(StorageErrorKind::InternalError, detail)
}

pub fn other_error(detail: Option<&str>) -> StorageError {
    StorageError::new(StorageErrorKind::Other, detail)
}
