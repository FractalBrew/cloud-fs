//! The main types used in this crate.

extern crate bytes;

pub(crate) mod error;
pub(crate) mod future;
pub(crate) mod objects;
pub(crate) mod path;
pub(crate) mod stream;

use bytes::Bytes;

use super::filestore::FileStore;
pub use error::{FsError, FsErrorKind, FsResult};
pub use future::WrappedFuture;
pub use objects::{Object, ObjectType};
pub use path::StoragePath;
pub use stream::WrappedStream;

/// The data type used for streaming data from and to files.
pub type Data = Bytes;

/// A stream that returns [`Data`](type.Data.html).
pub type DataStream = WrappedStream<FsResult<Data>>;
/// A future that returns a [FileStore](struct.FileStore.html) implementation.
pub type ConnectFuture = WrappedFuture<FsResult<FileStore>>;
/// A stream that returns [`Object`s](struct.Object.html).
pub type ObjectStream = WrappedStream<FsResult<Object>>;
/// A future that returns an [`ObjectStream`](type.ObjectStream.html).
pub type ObjectStreamFuture = WrappedFuture<FsResult<ObjectStream>>;
/// A future that returns an [`Object`](type.Object.html).
pub type ObjectFuture = WrappedFuture<FsResult<Object>>;
/// A future that resolves whenever the requested operation is complete.
pub type OperationCompleteFuture = WrappedFuture<FsResult<()>>;
/// A future that resolves to a [`DataStream`](type.DataStream.html).
pub type DataStreamFuture = WrappedFuture<FsResult<DataStream>>;
