//! Contains the different storage backend implementations.
//!
//! Each backend allows for accessing files in a different storage system.
//! Normally you just crate a [`FileStore`](../struct.FileStore.html) from the
//! backend and then everything else is done by calls to the `FileStore` which
//! generally behave the same regardless of the backend.
#[cfg(feature = "b2")]
pub mod b2;
#[cfg(feature = "file")]
pub mod file;

use std::convert::TryInto;
use std::error::Error;
use std::fmt;

use bytes::IntoBuf;
use futures::future::TryFutureExt;
use futures::stream::Stream;

use crate::types::*;

/// An enumeration of the available backends.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Backend {
    #[cfg(feature = "file")]
    /// The [file backend](file/index.html). Included with the "file" feature.
    File,
    #[cfg(feature = "b2")]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ObjectInternals {
    #[cfg(feature = "file")]
    File,
    #[cfg(feature = "b2")]
    B2(b2::B2ObjectInternals),
}

impl ObjectInternals {
    pub(crate) fn from_backend(&self) -> Backend {
        match self {
            #[cfg(feature = "file")]
            ObjectInternals::File => Backend::File,
            #[cfg(feature = "b2")]
            ObjectInternals::B2(_) => Backend::B2,
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
    File(Box<file::FileBackend>),
    #[cfg(feature = "b2")]
    /// The [file backend](struct.FileBackend.html).
    B2(Box<b2::B2Backend>),
}

/// The trait that every storage backend must implement at a minimum.
pub trait StorageBackend: Clone + Send + 'static {
    /// See [`backend_type`](../../struct.FileStore.html#method.backend_type).
    fn backend_type(&self) -> Backend;

    /// See [`list_objects`](../../struct.FileStore.html#method.list_objects).
    fn list_objects<P>(&self, prefix: P) -> ObjectStreamFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>;

    /// See [`list_directory`](../../struct.FileStore.html#method.list_directory).
    fn list_directory<P>(&self, dir: P) -> ObjectStreamFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>;

    /// See [`get_object`](../../struct.FileStore.html#method.get_object).
    fn get_object<P>(&self, path: P) -> ObjectFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>;

    /// See [`get_file_stream`](../../struct.FileStore.html#method.get_file_stream).
    fn get_file_stream<O>(&self, reference: O) -> DataStreamFuture
    where
        O: ObjectReference;

    /// See [`copy_file`](../../struct.FileStore.html#method.copy_file).
    fn copy_file<O, P>(&self, reference: O, target: P) -> CopyCompleteFuture
    where
        O: ObjectReference,
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        let source = DataStream::from_stream(self.get_file_stream(reference).try_flatten_stream());
        self.write_file_from_stream(target, source)
    }

    /// See [`move_file`](../../struct.FileStore.html#method.move_file).
    fn move_file<O, P>(&self, reference: O, target: P) -> MoveCompleteFuture
    where
        O: ObjectReference,
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        let deleter = self.clone();
        MoveCompleteFuture::from_future(self.copy_file(reference.clone(), target).and_then(
            move |()| {
                deleter
                    .delete_object(reference)
                    .map_err(TransferError::SourceError)
            },
        ))
    }

    /// See [`delete_object`](../../struct.FileStore.html#method.delete_object).
    fn delete_object<O>(&self, reference: O) -> OperationCompleteFuture
    where
        O: ObjectReference;

    /// See [`write_file_from_stream`](../../struct.FileStore.html#method.write_file_from_stream).
    fn write_file_from_stream<S, I, E, P>(&self, path: P, stream: S) -> WriteCompleteFuture
    where
        S: Stream<Item = Result<I, E>> + Send + 'static,
        I: IntoBuf + 'static,
        E: 'static + Error + Send + Sync,
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>;
}
