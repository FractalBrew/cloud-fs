use std::error::Error;
use std::fmt;

use super::*;

/// The type of an [`FsError`](struct.FsError.html).
#[derive(Clone, Debug, PartialEq)]
pub enum FsErrorKind {
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
    /// An unknown error type, usually a marker that this `FsError` was
    /// generated from a different error type.
    Unknown,
}

/// The main error type used throughout this crate.
#[derive(Clone, Debug)]
pub struct FsError {
    kind: FsErrorKind,
    description: String,
}

impl FsError {
    pub(crate) fn parse_error(source: &str, description: &str) -> FsError {
        FsError {
            kind: FsErrorKind::ParseError(source.to_owned()),
            description: format!(
                "Failed while parsing '{}': {}",
                source,
                description.to_owned()
            ),
        }
    }

    /*pub(crate) fn address_not_supported(address: &Address, description: &str) -> FsError {
        FsError {
            kind: FsErrorKind::AddressNotSupported(address.clone()),
            description: description.to_owned(),
        }
    }*/

    pub(crate) fn invalid_path(path: StoragePath, description: &str) -> FsError {
        FsError {
            description: format!("Path '{}' was invalid: {}", path, description),
            kind: FsErrorKind::InvalidPath(path),
        }
    }

    pub(crate) fn not_found(path: StoragePath) -> FsError {
        FsError {
            description: format!("File at '{}' was not found.", path),
            kind: FsErrorKind::NotFound(path),
        }
    }

    pub(crate) fn invalid_settings(description: &str) -> FsError {
        FsError {
            kind: FsErrorKind::InvalidSettings,
            description: description.to_owned(),
        }
    }

    pub(crate) fn unknown<E>(error: E) -> FsError
    where
        E: Error,
    {
        FsError {
            kind: FsErrorKind::Unknown,
            description: format!("{}", error),
        }
    }

    /// Gets the [`FsErrorKind`](enum.FsErrorKind.html) of this `FsError`.
    pub fn kind(&self) -> FsErrorKind {
        self.kind.clone()
    }
}

impl fmt::Display for FsError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(&self.description)
    }
}

impl Error for FsError {}

/// A simple alias for a `Result` where the error is an [`FsError`](struct.FsError.html).
pub type FsResult<R> = Result<R, FsError>;
