use std::error::Error;
use std::fmt;

use super::*;

/// The type of a [`StorageError`](struct.StorageError.html).
#[derive(Clone, Debug, PartialEq)]
pub enum StorageErrorKind {
    /// An error that occuring while parsing or manipulating a
    /// [`StoragePath`](struct.StoragePath.html].
    ParseError(String),

    // An error a backend may return if an invalid storage host was requested.
    // AddressNotSupported(Address),
    /// An error returned when attempting to access an invalid path.
    InvalidPath(StoragePath),
    /// The object requested was not found.
    NotFound(StoragePath),
    /// An error returned if configuration for a backend was invalid somehow.
    InvalidSettings,
    /// An unknown error type, usually a marker that this `StorageError` was
    /// generated from a different error type.
    Unknown,
}

/// The main error type used throughout this crate.
#[derive(Clone, Debug)]
pub struct StorageError {
    kind: StorageErrorKind,
    description: String,
}

impl StorageError {
    pub(crate) fn parse_error(source: &str, description: &str) -> StorageError {
        StorageError {
            kind: StorageErrorKind::ParseError(source.to_owned()),
            description: format!(
                "Failed while parsing '{}': {}",
                source,
                description.to_owned()
            ),
        }
    }

    /*pub(crate) fn address_not_supported(address: &Address, description: &str) -> StorageError {
        StorageError {
            kind: StorageErrorKind::AddressNotSupported(address.clone()),
            description: description.to_owned(),
        }
    }*/

    pub(crate) fn invalid_path(path: StoragePath, description: &str) -> StorageError {
        StorageError {
            description: format!("Path '{}' was invalid: {}", path, description),
            kind: StorageErrorKind::InvalidPath(path),
        }
    }

    pub(crate) fn not_found(path: StoragePath) -> StorageError {
        StorageError {
            description: format!("File at '{}' was not found.", path),
            kind: StorageErrorKind::NotFound(path),
        }
    }

    pub(crate) fn invalid_settings(description: &str) -> StorageError {
        StorageError {
            kind: StorageErrorKind::InvalidSettings,
            description: description.to_owned(),
        }
    }

    pub(crate) fn unknown<E>(error: E) -> StorageError
    where
        E: Error,
    {
        StorageError {
            kind: StorageErrorKind::Unknown,
            description: format!("{}", error),
        }
    }

    /// Gets the [`StorageErrorKind`](enum.StorageErrorKind.html) of this `StorageError`.
    pub fn kind(&self) -> StorageErrorKind {
        self.kind.clone()
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(&self.description)
    }
}

impl Error for StorageError {}

/// A simple alias for a `Result` where the error is a [`StorageError`](struct.StorageError.html).
pub type StorageResult<R> = Result<R, StorageError>;
