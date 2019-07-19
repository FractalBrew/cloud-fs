//! Contains the different storage backend implementations.
#[cfg(feature = "file")]
mod file;

use std::fmt;
use std::io;

#[cfg(feature = "file")]
pub use file::FileBackend;

use crate::types::stream::WrappedStream;
use crate::types::*;

/// An enumeration of the available backends.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Backend {
    #[cfg(feature = "file")]
    /// The [file backend](file/index.html). Included with the "file" feature.
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

    /// Lists the objects that start with the given path.
    ///
    /// See [`FileStore.list_objects`](../struct.FileStore.html#method.list_objects).
    fn list_objects(&self, path: StoragePath) -> ObjectStreamFuture;

    /// Gets info about the object at the given path.
    ///
    /// See [`FileStore.get_object`](../struct.FileStore.html#method.get_object).
    fn get_object(&self, path: StoragePath) -> ObjectFuture;

    /// Gets a stream of data for the file at the given path. Fails if the path
    /// does not point to a file.
    ///
    /// See [`FileStore.get_file_stream`](../struct.FileStore.html#method.get_file_stream).
    fn get_file_stream(&self, path: StoragePath) -> DataStreamFuture;

    /// Deletes the object at the given path.
    ///
    /// See [`FileStore.delete_object`](../struct.FileStore.html#method.delete_object).
    fn delete_object(&self, path: StoragePath) -> OperationCompleteFuture;

    /// Writes a stream of data the the file at the given path.
    ///
    /// See [`FileStore.write_file_from_stream`](../struct.FileStore.html#method.write_file_from_stream).
    fn write_file_from_stream(
        &self,
        path: StoragePath,
        stream: WrappedStream<Result<Data, io::Error>>,
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
