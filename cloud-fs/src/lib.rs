//! An abstract asynchronous API for accessing a filesystem on multiple different local and cloud storage backends.
//!
//! The API offers functions for listing, reading, writing and deleting files
//! from a storage backend. Each backend offers the same API plus in some cases
//! some additional backend specific functionality.
//!
//! Which backend is available depends on the features cloud-fs is compiled
//! with, by default all are included. See the [backends module](backends/index.html)
//! for a list of the backends.
extern crate bytes;
extern crate tokio;

pub mod backends;
mod futures;
mod types;

use std::error::Error;

use tokio::prelude::*;
use bytes::{Bytes, IntoBuf};
use bytes::buf::FromBuf;

pub use types::{FsPath, FsSettings, FsError};
use backends::{connect, Backend};
use futures::*;

trait FsImpl {
    fn list_files(&self, path: &FsPath) -> FileListStream;

    fn get_file(&self, path: &FsPath) -> FileFuture;

    fn delete_file(&self, path: &FsPath) -> OperationCompleteFuture;

    fn get_file_stream(&self, path: &FsPath) -> DataStreamFuture;

    fn write_from_stream(&self, path: &FsPath, stream: DataStream) -> OperationCompleteFuture;
}

#[derive(Debug)]
pub struct Fs {
    backend: Backend,
}

impl Fs {
    pub fn new(settings: FsSettings) -> ConnectFuture {
        connect(settings)
    }

    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    pub fn list_files<P>(&self, path: P) -> FileListStream
    where
        P: AsRef<FsPath>,
    {
        self.backend.get().list_files(path.as_ref())
    }

    pub fn get_file<P>(&self, path: P) -> FileFuture
    where
        P: AsRef<FsPath>,
    {
        self.backend.get().get_file(path.as_ref())
    }

    pub fn delete_file<P>(&self, path: P) -> OperationCompleteFuture
    where
        P: AsRef<FsPath>,
    {
        self.backend.get().delete_file(path.as_ref())
    }

    pub fn get_file_stream<P>(&self, path: P) -> DataStreamFuture
    where
        P: AsRef<FsPath>,
    {
        self.backend.get().get_file_stream(path.as_ref())
    }

    pub fn write_from_stream<P, S, I, E>(&self, path: P, stream: S) -> OperationCompleteFuture
    where
        P: AsRef<FsPath>,
        S: Stream<Item = I, Error = E> + 'static,
        I: IntoBuf,
        E: Error,
    {
        let mapped = stream
            .map(|i| Bytes::from_buf(i))
            .map_err(|e| FsError::from_error(e));

        self.backend
            .get()
            .write_from_stream(path.as_ref(), DataStream::from_stream(mapped))
    }
}
