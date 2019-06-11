//! An abstract asynchronous API for accessing a filesystem that coukd be on any of a number of different local and cloud storage backends.
//!
//! The API offers functions for listing, reading, writing and deleting files
//! from a storage backend. Each backend offers the same API plus in some cases
//! some additional backend specific functionality.
//!
//! Obviously offering the same API across all backends means the API is fairly
//! basic, but if all you want to do is write, read or list files is should be
//! plenty.
//!
//! Which backend is available depends on the features cloud-fs is compiled
//! with, by default all are included. See the [backends module](backends/index.html)
//! for a list of the backends.
// #![warn(missing_docs)]
// #![warn(clippy::missing_docs_in_private_items)]

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

pub use backends::{BackendImplementation, Backend};
use backends::connect;
use futures::*;
pub use types::{FsError, FsErrorType, FsPath, FsSettings, FsResult, FsFile};

/// The trait that every storage backend must implement at a minimum.
trait FsImpl {
    /// Lists the files that start with the given path.
    ///
    /// See [Fs.list_files](struct.Fs.html#method.list_files).
    fn list_files(&self, path: &FsPath) -> FileListFuture;

    /// Gets info about the file at the given path.
    ///
    /// See [Fs.get_file](struct.Fs.html#method.get_file).
    fn get_file(&self, path: &FsPath) -> FileFuture;

    /// Deletes the file at the given path.
    ///
    /// See [Fs.get_file](struct.Fs.html#method.delete_file).
    fn delete_file(&self, path: &FsPath) -> OperationCompleteFuture;

    /// Gets a stream of data for the file at the given path.
    ///
    /// See [Fs.get_file](struct.Fs.html#method.get_file_stream).
    fn get_file_stream(&self, path: &FsPath) -> DataStreamFuture;

    /// Writes a stram of data the the file at the given path.
    ///
    /// See [Fs.get_file](struct.Fs.html#method.write_from_stream).
    fn write_from_stream(&self, path: &FsPath, stream: DataStream) -> OperationCompleteFuture;
}

#[derive(Debug)]
pub struct Fs {
    backend: BackendImplementation,
}

impl Fs {
    fn check_path(&self, path: &FsPath, should_be_dir: bool) -> FsResult<()> {
        if !path.is_absolute() {
            Err(FsError::new(FsErrorType::InvalidPath, "Requests must use an absolute path."))
        } else if should_be_dir && !path.is_directory() {
            Err(FsError::new(FsErrorType::InvalidPath, "This request requires the path to a directory."))
        } else if !should_be_dir && path.is_directory() {
            Err(FsError::new(FsErrorType::InvalidPath, "This request requires the path to a file."))
        } else if path.is_windows() {
            Err(FsError::new(FsErrorType::InvalidPath, "Paths should not include windows prefixes."))
        } else {
            Ok(())
        }
    }

    /// Create a new `Fs` based on the settings passed.
    pub fn new(settings: FsSettings) -> ConnectFuture {
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
    pub fn list_files<P>(&self, path: P) -> FileListFuture
    where
        P: AsRef<FsPath>,
    {
        if let Err(e) = self.check_path(path.as_ref(), true) {
            return FileListFuture::from_error(e);
        }

        self.backend.get().list_files(path.as_ref())
    }

    /// Gets info about the file at the given path.
    ///
    /// This will return an error if the file does not exist.
    pub fn get_file<P>(&self, path: P) -> FileFuture
    where
        P: AsRef<FsPath>,
    {
        if let Err(e) = self.check_path(path.as_ref(), false) {
            return FileFuture::from_error(e);
        }

        self.backend.get().get_file(path.as_ref())
    }

    /// Deletes the file at the given path.
    ///
    /// This will not resolve to an error if the file already does not exist. It
    /// will return an error if the attempt to delete the file failed.
    pub fn delete_file<P>(&self, path: P) -> OperationCompleteFuture
    where
        P: AsRef<FsPath>,
    {
        if let Err(e) = self.check_path(path.as_ref(), false) {
            return OperationCompleteFuture::from_error(e);
        }

        self.backend.get().delete_file(path.as_ref())
    }

    /// Gets a stream of data for the file at the given path.
    ///
    /// The data returned is not necessarily in any particular chunk size.
    /// Dropping the stream at any point before completion should be considered
    /// to be safe.
    pub fn get_file_stream<P>(&self, path: P) -> DataStreamFuture
    where
        P: AsRef<FsPath>,
    {
        if let Err(e) = self.check_path(path.as_ref(), false) {
            return DataStreamFuture::from_error(e);
        }

        self.backend.get().get_file_stream(path.as_ref())
    }

    /// Writes a stream of data the the file at the given path.
    ///
    /// The future returned will only resolve once all the data from the stream
    /// is succesfully written to storage. If the stream resolves to None at any
    /// point this will be considered the end of the data to be written.
    pub fn write_from_stream<P, S, I, E>(&self, path: P, stream: S) -> OperationCompleteFuture
    where
        P: AsRef<FsPath>,
        S: Stream<Item = I, Error = E> + Send + Sync + 'static,
        I: IntoBuf,
        E: Error,
    {
        if let Err(e) = self.check_path(path.as_ref(), false) {
            return OperationCompleteFuture::from_error(e);
        }

        #[allow(clippy::redundant_closure)]
        let mapped = stream
            .map(|i| Bytes::from_buf(i))
            .map_err(|e| FsError::from_error(e));

        self.backend
            .get()
            .write_from_stream(path.as_ref(), DataStream::from_stream(mapped))
    }
}
