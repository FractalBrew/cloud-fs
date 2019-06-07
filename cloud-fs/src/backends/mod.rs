#[cfg(feature = "b2")]
mod b2;
#[cfg(feature = "file")]
mod file;

use crate::{ConnectFuture, FsImpl, FsSettings};

#[cfg(feature = "b2")]
pub use b2::B2Backend;
#[cfg(feature = "file")]
pub use file::FileBackend;

#[derive(Clone, Debug)]
pub enum Backend {
    #[cfg(feature = "file")]
    File,
    #[cfg(feature = "b2")]
    B2,
}

#[derive(Debug)]
pub enum BackendImplementation {
    #[cfg(feature = "file")]
    File(FileBackend),
    #[cfg(feature = "b2")]
    B2(B2Backend),
}

impl BackendImplementation {
    pub(crate) fn get(&self) -> Box<&FsImpl> {
        match self {
            #[cfg(feature = "file")]
            BackendImplementation::File(ref fs) => Box::new(fs),
            #[cfg(feature = "b2")]
            BackendImplementation::B2(ref fs) => Box::new(fs),
        }
    }
}

pub(crate) fn connect(settings: FsSettings) -> ConnectFuture {
    match settings.backend() {
        #[cfg(feature = "file")]
        Backend::File => FileBackend::connect(settings),
        #[cfg(feature = "b2")]
        Backend::B2 => B2Backend::connect(settings),
    }
}
