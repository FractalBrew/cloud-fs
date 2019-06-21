//! An abstract asynchronous API for accessing a filesystem that could be on any of a number of different local and cloud storage backends.
//!
//! The API offers functions for listing, reading, writing and deleting files
//! from a storage backend. Each backend offers the same API plus in some cases
//! some additional backend specific functionality.
//!
//! Obviously offering the same API across all backends means the API is fairly
//! basic, but if all you want to do is write, read or list files it should be
//! plenty.
//!
//! Which backend is available depends on the features cloud-fs is compiled
//! with, by default all are included. See the [backends module](backends/index.html)
//! for a list of the backends.
//!
//! The [`Fs`](struct.Fs.html) is the main API used to access storage.
#![warn(missing_docs)]

extern crate bytes;
extern crate tokio;

pub mod backends;
mod futures;
mod types;
pub mod utils;

use std::error::Error;

use bytes::buf::FromBuf;
use bytes::{Bytes, IntoBuf};
use tokio::prelude::*;

use backends::connect;
use backends::*;
pub use futures::*;
pub use types::{Data, FsError, FsErrorKind, FsFile, FsPath, FsResult, FsSettings};

/// The trait that every storage backend must implement at a minimum.
trait FsImpl {
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
    fn write_from_stream(&self, path: FsPath, stream: DataStream) -> OperationCompleteFuture;
}

/// The main implementation used to interact with a storage backend.
#[derive(Debug)]
pub struct Fs {
    backend: BackendImplementation,
}

impl Fs {
    fn check_path(&self, path: &FsPath, should_be_dir: bool) -> FsResult<()> {
        if !path.is_absolute() {
            Err(FsError::new(
                FsErrorKind::InvalidPath,
                "Requests must use an absolute path.",
            ))
        } else if should_be_dir && !path.is_directory() {
            Err(FsError::new(
                FsErrorKind::InvalidPath,
                "This request requires the path to a directory.",
            ))
        } else if !should_be_dir && path.is_directory() {
            Err(FsError::new(
                FsErrorKind::InvalidPath,
                "This request requires the path to a file.",
            ))
        } else if path.is_windows() {
            Err(FsError::new(
                FsErrorKind::InvalidPath,
                "Paths should not include windows prefixes.",
            ))
        } else {
            Ok(())
        }
    }

    /// Connect to a `Fs` based on the settings passed.
    pub fn connect(settings: FsSettings) -> ConnectFuture {
        if !settings.path.is_absolute() {
            return ConnectFuture::from_error(FsError::new(
                FsErrorKind::InvalidSettings,
                "Fs must be initialized with an absolute path.",
            ));
        } else if !settings.path.is_directory() {
            return ConnectFuture::from_error(FsError::new(
                FsErrorKind::InvalidSettings,
                "Fs must be initialized with a directory path.",
            ));
        }

        connect(settings)
    }

    /// Retrieves the back-end that this `Fs` is using.
    ///
    /// This is generally only useful for accessing back-end specific
    /// functionality. If you want to develop a truly back-end agnostic app then
    /// you should avoid calling this.
    pub fn backend(&self) -> &BackendImplementation {
        &self.backend
    }

    /// Lists the files that start with the given path.
    ///
    /// Because the majority of cloud storage systems do not really have a
    /// notion of directories and files, just file identifiers, this function
    /// will return any files that have an identifier prefixed by `path`.
    pub fn list_files(&self, path: FsPath) -> FileListFuture {
        if let Err(e) = self.check_path(&path, true) {
            return FileListFuture::from_error(e);
        }

        self.backend.get().list_files(path)
    }

    /// Gets info about the file at the given path.
    ///
    /// This will return a [`NotFound`](enum.FsErrorKind.html#variant.NotFound)
    /// error if the file does not exist.
    pub fn get_file(&self, path: FsPath) -> FileFuture {
        if let Err(e) = self.check_path(&path, false) {
            return FileFuture::from_error(e);
        }

        self.backend.get().get_file(path)
    }

    /// Gets a stream of data for the file at the given path.
    ///
    /// The data returned is not necessarily in any particular chunk size.
    /// Dropping the stream at any point before completion should be considered
    /// to be safe.
    ///
    /// This will return a [`NotFound`](enum.FsErrorKind.html#variant.NotFound)
    /// error if the file does not exist.
    pub fn get_file_stream(&self, path: FsPath) -> DataStreamFuture {
        if let Err(e) = self.check_path(&path, false) {
            return DataStreamFuture::from_error(e);
        }

        self.backend.get().get_file_stream(path)
    }

    /// Deletes the file at the given path.
    ///
    /// This will return a [`NotFound`](enum.FsErrorKind.html#variant.NotFound)
    /// error if the file does not exist.
    pub fn delete_file(&self, path: FsPath) -> OperationCompleteFuture {
        if let Err(e) = self.check_path(&path, false) {
            return OperationCompleteFuture::from_error(e);
        }

        self.backend.get().delete_file(path)
    }

    /// Writes a stream of data the the file at the given path.
    ///
    /// The future returned will only resolve once all the data from the stream
    /// is succesfully written to storage. If the provided stream resolves to
    /// None at any point this will be considered the end of the data to be
    /// written.
    pub fn write_from_stream<S, I, E>(&self, path: FsPath, stream: S) -> OperationCompleteFuture
    where
        S: Stream<Item = I, Error = E> + Send + Sync + 'static,
        I: IntoBuf,
        E: Error,
    {
        if let Err(e) = self.check_path(&path, false) {
            return OperationCompleteFuture::from_error(e);
        }

        #[allow(clippy::redundant_closure)]
        let mapped = stream
            .map(|i| Bytes::from_buf(i))
            .map_err(|e| FsError::from_error(e));

        self.backend
            .get()
            .write_from_stream(path, DataStream::from_stream(mapped))
    }
}
