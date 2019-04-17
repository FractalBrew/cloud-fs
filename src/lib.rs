use std::io;
use std::fmt;
use std::error;
use url::Url;

mod schemes;

#[derive(Debug)]
pub struct Error {
    message: String,
}

impl Error {
    fn new(message: &str) -> Error {
        Error {
            message: String::from(message),
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

#[derive(PartialEq, Clone)]
enum FileKind {
    File,
    Directory,
    Symlink,
}

#[derive(Clone)]
pub struct FileType {
    kind: FileKind,
}

impl FileType {
    pub fn is_dir(&self) -> bool {
        self.kind == FileKind::Directory
    }

    pub fn is_file(&self) -> bool {
        self.kind == FileKind::File
    }

    pub fn is_symlink(&self) -> bool {
        self.kind == FileKind::Symlink
    }
}

pub struct Metadata {
    kind: FileKind,
    length: u64,
}

impl Metadata {
    pub fn file_type(&self) -> FileType {
        FileType {
            kind: self.kind.clone(),
        }
    }

    pub fn is_dir(&self) -> bool {
        self.kind == FileKind::Directory
    }

    pub fn is_file(&self) -> bool {
        self.kind == FileKind::File
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn len(&self) -> u64 {
        self.length
    }
}

pub struct ReadDir {
}

impl ReadDir {
}

pub trait TryIntoUrl {
    fn try_into_url(&self) -> Result<Url, url::ParseError>;
}

impl TryIntoUrl for str {
    fn try_into_url(&self) -> Result<Url, url::ParseError> {
        Ok(self.parse::<Url>()?)
    }
}

impl TryIntoUrl for Url {
    fn try_into_url(&self) -> Result<Url, url::ParseError> {
        Ok(self.clone())
    }
}

trait FsImplementation {
    fn get_root(&self) -> &Url;

    fn includes(&self, url: &Url) -> bool {
        true
    }

    fn to_absolute_url(&self, path: &str) -> io::Result<Url> {
        let root = self.get_root();
        let url = match root.join(path.trim_start_matches('/')) {
            Ok(url) => url,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
        };

        if self.includes(&url) {
            Ok(url)
        } else {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, Error::new("Cannot access files outside of the filesystem.")))
        }
    }

    fn metadata(&self, url: Url) -> io::Result<Metadata>;
    fn symlink_metadata(&self, url: Url) -> io::Result<Metadata>;
    fn read_dir(&self, url: Url) -> io::Result<ReadDir>;
    fn remove_dir_all(&self, url: Url) -> io::Result<()>;
    fn remove_file(&self, url: Url) -> io::Result<()>;
}

pub struct Fs {
    scheme: Box<FsImplementation>,
}

impl Fs {
    pub fn metadata(&self, path: &str) -> io::Result<Metadata> {
        self.scheme.metadata(self.scheme.to_absolute_url(path)?)
    }

    pub fn read_dir(&self, path: &str) -> io::Result<ReadDir> {
        self.scheme.read_dir(self.scheme.to_absolute_url(path)?)
    }

    pub fn remove_dir_all(&self, path: &str) -> io::Result<()> {
        self.scheme.remove_dir_all(self.scheme.to_absolute_url(path)?)
    }

    pub fn remove_file(&self, path: &str) -> io::Result<()> {
        self.scheme.remove_file(self.scheme.to_absolute_url(path)?)
    }
}

pub fn open_fs<U: TryIntoUrl>(target: &U) -> io::Result<Fs> {
    let url = match target.try_into_url() {
        Ok(url) => url,
        Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
    };

    match url.scheme() {
        "file" => schemes::file::FileFs::connect(url),
        _ => Err(io::Error::new(io::ErrorKind::NotFound, Error::new(&format!("Scheme {} is unsupported.", url.scheme())))),
    }
}
