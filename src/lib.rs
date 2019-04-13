use std::io;
use std::path::Path;
use std::fmt;
use std::error;
use http::uri::Uri;

mod schemes;

#[derive(Debug)]
pub struct Error {
    message: String,
    source: Option<Box<dyn error::Error+Send+Sync>>,
}

impl Error {
    fn new(message: &str) -> Error {
        Error {
            message: String::from(message),
            source: None,
        }
    }

    fn wrap<E>(message: &str, error: E) -> Error
        where E: Into<Box<dyn error::Error+Send+Sync>>
    {
        Error {
            message: String::from(message),
            source: Some(error.into()),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(&self.message)
    }
}

impl error::Error for Error {
}

pub struct Metadata {
}

impl Metadata {
}

pub struct ReadDir {
}

impl ReadDir {
}

pub trait FsImplementation {
    fn metadata(&self, path: &Path) -> io::Result<Metadata>;
    fn read_dir(&self, path: &Path) -> io::Result<ReadDir>;
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
}

pub struct Fs {
    scheme: Box<FsImplementation>,
}

impl Fs {
    fn metadata<P: AsRef<Path>>(&self, path: P) -> io::Result<Metadata> {
        self.scheme.metadata(path.as_ref())
    }

    fn read_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<ReadDir> {
        self.scheme.read_dir(path.as_ref())
    }

    fn remove_dir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        self.scheme.remove_dir_all(path.as_ref())
    }

    fn remove_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        self.scheme.remove_file(path.as_ref())
    }
}

pub fn open_fs(target: &str) -> Result<Fs, Error> {
    let uriresult = target.parse::<Uri>();
    let uri = match uriresult {
        Ok(uri) => uri,
        Err(e) => return Err(Error::wrap("Unable to parse target.", e)),
    };

    match uri.scheme_str() {
        Some("file") => schemes::file::FileFs::new(uri),
        Some(scheme) => Err(Error::new(&format!("Scheme {} is unsupported.", scheme))),
        None => Err(Error::new("Target did not include a scheme.")),
    }
}
