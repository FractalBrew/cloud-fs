//! Contains the different storage backend implementations.
#[cfg(feature = "b2")]
mod b2;
#[cfg(feature = "file")]
mod file;

use std::fmt;

use futures::future::TryFutureExt;

#[cfg(feature = "b2")]
pub use b2::B2Backend;
#[cfg(feature = "file")]
pub use file::FileBackend;

use crate::types::*;

/// An enumeration of the available backends.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Backend {
    #[cfg(feature = "file")]
    /// The [file backend](file/index.html). Included with the "file" feature.
    File,
    #[cfg(feature = "file")]
    /// The [b2 backend](b2/index.html). Included with the "b2" feature.
    B2,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            #[cfg(feature = "file")]
            Backend::File => f.pad("file"),
            #[cfg(feature = "b2")]
            Backend::B2 => f.pad("b2"),
        }
    }
}

macro_rules! call_backend {
    ($backend:expr, $method:ident) => {
        match $backend {
            #[cfg(feature = "file")]
            BackendImplementation::File(b) => b.$method(),
            #[cfg(feature = "b2")]
            BackendImplementation::B2(b) => b.$method(),
        }
    };
    ($backend:expr, $method:ident, $($arg:expr),*) => {
        match $backend {
            #[cfg(feature = "file")]
            BackendImplementation::File(b) => b.$method($($arg,)*),
            #[cfg(feature = "b2")]
            BackendImplementation::B2(b) => b.$method($($arg,)*),
        }
    };
}

/// Holds a backend implementation.
#[derive(Clone, Debug)]
pub(crate) enum BackendImplementation {
    #[cfg(feature = "file")]
    /// The [file backend](struct.FileBackend.html).
    File(Box<FileBackend>),
    #[cfg(feature = "b2")]
    /// The [file backend](struct.FileBackend.html).
    B2(Box<B2Backend>),
}

/// The trait that every storage backend must implement at a minimum.
pub(crate) trait StorageImpl: Clone + Send + 'static {
    /// Returns the type of backend.
    fn backend_type(&self) -> Backend;

    /// Lists the objects that start with the given path.
    ///
    /// See [`FileStore.list_objects`](../struct.FileStore.html#method.list_objects).
    fn list_objects(&self, path: ObjectPath) -> ObjectStreamFuture;

    /// Gets info about the object at the given path.
    ///
    /// See [`FileStore.get_object`](../struct.FileStore.html#method.get_object).
    fn get_object(&self, path: ObjectPath) -> ObjectFuture;

    /// Gets a stream of data for the file at the given path. Fails if the path
    /// does not point to a file.
    ///
    /// See [`FileStore.get_file_stream`](../struct.FileStore.html#method.get_file_stream).
    fn get_file_stream(&self, path: ObjectPath) -> DataStreamFuture;

    /// Copies a file to a new path.
    ///
    /// See [`FileStore.copy_file`](../struct.FileStore.html#method.copy_file).
    fn copy_file(&self, path: ObjectPath, target: ObjectPath) -> CopyCompleteFuture {
        let source = DataStream::from_stream(self.get_file_stream(path).try_flatten_stream());
        self.write_file_from_stream(target, source)
    }

    /// Moves a file to a new path.
    ///
    /// See [`FileStore.move_file`](../struct.FileStore.html#method.move_file).
    fn move_file(&self, path: ObjectPath, target: ObjectPath) -> MoveCompleteFuture {
        let deleter = self.clone();
        MoveCompleteFuture::from_future(self.copy_file(path.clone(), target).and_then(move |()| {
            deleter
                .delete_object(path)
                .map_err(TransferError::SourceError)
        }))
    }

    /// Deletes the object at the given path.
    ///
    /// See [`FileStore.delete_object`](../struct.FileStore.html#method.delete_object).
    fn delete_object(&self, path: ObjectPath) -> OperationCompleteFuture;

    /// Writes a stream of data into the file at the given path.
    ///
    /// See [`FileStore.write_file_from_stream`](../struct.FileStore.html#method.write_file_from_stream).
    fn write_file_from_stream(&self, path: ObjectPath, stream: DataStream) -> WriteCompleteFuture;
}
