use std::error::Error;
use std::fmt;
use std::net::IpAddr;

use bytes::Bytes;

use crate::backends::BackendType;

pub type Data = Bytes;

#[derive(Clone, Debug)]
pub enum FsErrorType {
    HostNotSupported,
    Other,
}

#[derive(Clone, Debug)]
pub struct FsError {
    error_type: FsErrorType,
    description: String,
}

impl FsError {
    pub(crate) fn new<S: AsRef<str>>(error_type: FsErrorType, description: S) -> FsError {
        FsError {
            error_type,
            description: description.as_ref().to_owned(),
        }
    }

    pub(crate) fn from_error<E>(error: E) -> FsError
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

#[derive(Clone, Debug)]
pub struct FsPath {}

#[derive(Clone, Debug)]
pub enum FsHost {
    HostName(String),
    Address(IpAddr),
}

#[derive(Clone, Debug)]
struct FsTarget {
    host: FsHost,
    port: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct FsSettings {
    backend: BackendType,
    target: Option<FsTarget>,
    path: FsPath,
}

impl FsSettings {
    pub fn backend(&self) -> &BackendType {
        &self.backend
    }

    pub fn hostname(&self) -> Option<&FsHost> {
        self.target.as_ref().map(|h| &h.host)
    }
}

#[derive(Debug)]
pub struct File {}
