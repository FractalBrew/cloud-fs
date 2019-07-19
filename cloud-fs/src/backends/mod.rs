//! Contains the different storage backend implementations.
#[cfg(feature = "file")]
mod file;

use std::fmt;
use std::io;

#[cfg(feature = "file")]
pub use file::FileBackend;

use crate::types::stream::StreamHolder;
use crate::types::*;

/// An enumeration of the available backends.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Backend {
    #[cfg(feature = "file")]
    /// The (file backend)[file/index.html]. Included with the "file" feature.
    File,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            #[cfg(feature = "file")]
            Backend::File => f.write_str("file"),
        }
    }
}

/// Holds a backend implementation.
#[derive(Debug)]
pub(crate) enum BackendImplementation {
    #[cfg(feature = "file")]
    /// The [file backend](struct.FileBackend.html).
    File(FileBackend),
}

/// The trait that every storage backend must implement at a minimum.
pub(crate) trait FsImpl {
    /// Returns the type of backend.
    fn backend_type(&self) -> Backend;

    /// Lists the files that start with the given path.
    ///
    /// See [Fs.list_files](struct.Fs.html#method.list_files).
    fn list_files(&self, path: FsPath) -> FileListFuture;

    /// Gets info about the file at the given path.
    ///
    /// See [Fs.get_file](struct.Fs.html#method.get_file).
    fn get_file(&self, path: FsPath) -> FileFuture;

    /// Gets a stream of data for the file at the given path.
    ///
    /// See [Fs.get_file](struct.Fs.html#method.get_file_stream).
    fn get_file_stream(&self, path: FsPath) -> DataStreamFuture;

    /// Deletes the file at the given path.
    ///
    /// See [Fs.get_file](struct.Fs.html#method.delete_file).
    fn delete_file(&self, path: FsPath) -> OperationCompleteFuture;

    /// Writes a stram of data the the file at the given path.
    ///
    /// See [Fs.get_file](struct.Fs.html#method.write_from_stream).
    fn write_from_stream(
        &self,
        path: FsPath,
        stream: StreamHolder<Result<Data, io::Error>>,
    ) -> OperationCompleteFuture;
}

impl BackendImplementation {
    pub(crate) fn get(&self) -> Box<&dyn FsImpl> {
        match self {
            #[cfg(feature = "file")]
            BackendImplementation::File(ref fs) => Box::new(fs),
        }
    }
}
