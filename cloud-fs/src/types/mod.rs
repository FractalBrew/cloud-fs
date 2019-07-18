mod path;

use std::cmp::{Ord, Ordering};
use std::error::Error;
use std::fmt;

use bytes::Bytes;

use crate::backends::Backend;
pub use path::FsPath;

/// The data type used for streaming data from and to files.
pub type Data = Bytes;

/// The type of an [`FsError`](struct.FsError.html).
#[derive(Clone, Debug, PartialEq)]
pub enum FsErrorKind {
    /// An error that occuring while parsing or manipulating an
    /// [`FsPath`](struct.FsPath.html].
    ParseError(String),

    /// An error a backend may return if an invalid storage host was requested.
    //AddressNotSupported(Address),
    /// An error returned when attempting to access an invalid path.
    InvalidPath(FsPath),
    /// The item requested was not found.
    NotFound(FsPath),
    /// An error returned if the [`FsSettings`](struct.FsSettings.html) is
    /// invalid in some way.
    InvalidSettings(FsSettings),
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

    pub(crate) fn invalid_path(path: &FsPath, description: &str) -> FsError {
        FsError {
            kind: FsErrorKind::InvalidPath(path.clone()),
            description: format!("Path '{}' was invalid: {}", path, description),
        }
    }

    pub(crate) fn not_found(path: &FsPath) -> FsError {
        FsError {
            kind: FsErrorKind::NotFound(path.clone()),
            description: format!("File at '{}' was not found.", path),
        }
    }

    pub(crate) fn invalid_settings(settings: &FsSettings, description: &str) -> FsError {
        FsError {
            kind: FsErrorKind::InvalidSettings(settings.to_owned()),
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

/*
#[derive(Clone, Debug, PartialEq)]
pub enum Host {
    Name(String),
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Host::Name(addr) => addr.fmt(f),
            Host::Ipv4(addr) => addr.fmt(f),
            Host::Ipv6(addr) => addr.fmt(f),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Address {
    pub host: Host,
    pub port: Option<u16>,
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.host.fmt(f)?;

        if let Some(p) = self.port {
            f.write_fmt(format_args!(":{}", p))?;
        }

        Ok(())
    }
}

impl From<SocketAddr> for Address {
    fn from(addr: SocketAddr) -> Address {
        match addr {
            SocketAddr::V4(addr4) => Address {
                host: Host::Ipv4(addr4.ip().to_owned()),
                port: Some(addr4.port()),
            },
            SocketAddr::V6(addr6) => Address {
                host: Host::Ipv6(addr6.ip().to_owned()),
                port: Some(addr6.port()),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Auth {
    pub username: String,
    pub password: String,
}
*/
/// Settings used to create an [`Fs`](struct.Fs.html) instance.
///
/// Different backends may interpret these settings in different ways. Check
/// the [`backends`](backends/index.html) for specific details.
#[derive(Clone, Debug, PartialEq)]
pub struct FsSettings {
    pub(crate) backend: Backend,
    //pub(crate) address: Option<Address>,
    //pub(crate) auth: Option<Auth>,
    pub(crate) path: FsPath,
}

impl FsSettings {
    /// Creates settings for a specific backend with a given [`FsPath`](struct.FsPath.html).
    pub fn new(backend: Backend, path: FsPath) -> FsSettings {
        FsSettings {
            backend,
            //address: None,
            //auth: None,
            path,
        }
    }

    /*
    /// Sets the address for the [`Fs`](struct.Fs.html).
    pub fn set_address<A>(&mut self, address: A)
    where
        A: Into<Address>,
    {
        self.address = Some(address.into());
    }

    /// Sets the authentication information for the [`Fs`](struct.Fs.html).
    pub fn set_authentication(&mut self, username: &str, password: &str) {
        self.auth = Some(Auth {
            username: username.to_owned(),
            password: password.to_owned(),
        });
    }*/

    /// Gets this setting's current [`Backend`](backends/enum.Backend.html).
    pub fn backend(&self) -> &Backend {
        &self.backend
    }
}

/// A file's type. For most backends this will just be File.
///
/// This crate really only deals with file manipulations and most backends only
/// support regular files and things like directories don't really exist.
/// In some cases though some backends do have real directories and would not
/// support creating a file of the same name. This gives the type of the file.
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum FsFileType {
    /// A regular file.
    File,
    /// A physical directory.
    Directory,
    /// An physical object of unknown type.
    Unknown,
}

impl Eq for FsFileType {}

impl PartialOrd for FsFileType {
    fn partial_cmp(&self, other: &FsFileType) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FsFileType {
    fn cmp(&self, other: &FsFileType) -> Ordering {
        if self == other {
            return Ordering::Equal;
        }

        match self {
            FsFileType::Directory => Ordering::Less,
            FsFileType::File => other.cmp(self),
            FsFileType::Unknown => Ordering::Greater,
        }
    }
}

/// A file in storage.
#[derive(Clone, PartialEq, Debug)]
pub struct FsFile {
    pub(crate) file_type: FsFileType,
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

    /// Gets the file's type.
    pub fn file_type(&self) -> FsFileType {
        self.file_type
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
