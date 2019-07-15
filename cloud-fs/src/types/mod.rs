mod path;

use std::cmp::{Ord, Ordering};
use std::error::Error;
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use bytes::Bytes;

use crate::backends::Backend;
pub use path::FsPath;

/// The data type used for streaming data from and to files.
pub type Data = Bytes;

/// The type of an [`FsError`](struct.FsError.html).
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FsErrorKind {
    /// An error that occuring while parsing or manipulating a
    /// [`FsPath`](struct.FsPath.html].
    ParseError,
    /// An error a backend may return if an invalid storage host was requested.
    HostNotSupported,
    /// An error returns when attempting to access an invalid path.
    InvalidPath,
    /// The item requested was not found.
    NotFound,
    /// An error returns if the [`FsSettings`](struct.FsSettings.html) is
    /// invalid in some way.
    InvalidSettings,
    /// An error used internally to mark a test failure.
    TestFailure,
    /// An error indicating that the requested function is not yet implemented.
    NotImplemented,
    /// An unknown error type, usually a marker that this `FsError` was
    /// generated from a different error type.
    Other,
}

/// The main error type used throughout this crate.
#[derive(Clone, Debug)]
pub struct FsError {
    kind: FsErrorKind,
    description: String,
}

impl FsError {
    /// Creates a new `FsError` instance
    pub fn new<S: AsRef<str>>(kind: FsErrorKind, description: S) -> FsError {
        FsError {
            kind,
            description: description.as_ref().to_owned(),
        }
    }

    /// Creates a new `FsError` out of some other kind of `Error`.
    pub fn from_error<E>(error: E) -> FsError
    where
        E: Error + fmt::Display,
    {
        Self::new(FsErrorKind::Other, format!("{}", error))
    }

    /// Gets the [`FsErrorKind`](enum.FsErrorKind.html) of this `FsError`.
    pub fn kind(&self) -> FsErrorKind {
        self.kind
    }

    pub(crate) fn not_found(path: &FsPath) -> FsError {
        FsError::new(FsErrorKind::NotFound, format!("{} does not exist.", path))
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

#[derive(Clone, Debug)]
pub enum Host {
    Name(String),
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            any => any.fmt(f),
        }
    }
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub(crate) struct Auth {
    pub username: String,
    pub password: String,
}

/// Settings used to create an [`Fs`](struct.Fs.html) instance.
///
/// Different backends may interpret these settings in different ways. Check
/// the [`backends`](backends/index.html) for specific details.
#[derive(Clone, Debug)]
pub struct FsSettings {
    pub(crate) backend: Backend,
    pub(crate) address: Option<Address>,
    pub(crate) auth: Option<Auth>,
    pub(crate) path: FsPath,
}

impl FsSettings {
    /// Creates settings for a specific backend with a given [`FsPath`](struct.FsPath.html).
    pub fn new(backend: Backend, path: FsPath) -> FsSettings {
        FsSettings {
            backend,
            address: None,
            auth: None,
            path,
        }
    }

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
