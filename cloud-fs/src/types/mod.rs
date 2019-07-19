extern crate bytes;

pub(crate) mod error;
pub(crate) mod future;
pub(crate) mod objects;
pub(crate) mod path;
pub(crate) mod stream;

use bytes::Bytes;

use super::fs::Fs;
pub use error::{FsError, FsErrorKind, FsResult};
pub use future::FsFuture;
pub use objects::{FsFile, FsFileType};
pub use path::FsPath;
pub use stream::FsStream;

/// The data type used for streaming data from and to files.
pub type Data = Bytes;

/// A stream that returns [`Data`](type.Data.html).
pub type DataStream = FsStream<Data>;
/// A future that returns a [`Fs`](struct.Fs.html) implementation.
pub type ConnectFuture = FsFuture<Fs>;
/// A stream that returns [`FsFile`s](struct.FsFile.html).
pub type FileListStream = FsStream<FsFile>;
/// A future that returns a [`FileListStream`](type.FileListStream.html).
pub type FileListFuture = FsFuture<FileListStream>;
/// A future that returns a [`FsFile`](type.FsFile.html).
pub type FileFuture = FsFuture<FsFile>;
/// A future that resolves whenever the requested operation is complete.
pub type OperationCompleteFuture = FsFuture<()>;
/// A future that resolves to a [`DataStream`](type.DataStream.html).
pub type DataStreamFuture = FsFuture<DataStream>;
