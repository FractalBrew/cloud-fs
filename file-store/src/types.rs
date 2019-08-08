//! The main types used in this crate.
pub(crate) mod error;
pub(crate) mod future;
pub(crate) mod objects;
pub(crate) mod path;
pub(crate) mod stream;

use std::io;

use bytes::buf::Buf;
use bytes::{Bytes, IntoBuf};
use futures::executor::{block_on_stream, BlockingStream};
use futures::stream::Stream;

use super::filestore::FileStore;
pub use error::{ObjectPathError, StorageError, StorageErrorKind, StorageResult, TransferError};
pub use future::WrappedFuture;
pub use objects::{Object, ObjectType};
pub use path::ObjectPath;
pub use stream::WrappedStream;

/// The data type used for streaming data from and to files.
pub type Data = Bytes;

/// A stream that returns [`Data`](type.Data.html).
pub type DataStream = WrappedStream<StorageResult<Data>>;
/// A future that returns a [FileStore](struct.FileStore.html) implementation.
pub type ConnectFuture = WrappedFuture<StorageResult<FileStore>>;
/// A stream that returns [`Object`s](struct.Object.html).
pub type ObjectStream = WrappedStream<StorageResult<Object>>;
/// A future that returns an [`ObjectStream`](type.ObjectStream.html).
pub type ObjectStreamFuture = WrappedFuture<StorageResult<ObjectStream>>;
/// A future that returns an [`Object`](struct.Object.html).
pub type ObjectFuture = WrappedFuture<StorageResult<Object>>;
/// A future that resolves whenever the requested operation is complete.
pub type OperationCompleteFuture = WrappedFuture<StorageResult<()>>;
/// A future that resolves when a write operation is complete.
pub type WriteCompleteFuture = WrappedFuture<Result<(), TransferError>>;
/// A future that resolves to a [`DataStream`](type.DataStream.html).
pub type DataStreamFuture = WrappedFuture<StorageResult<DataStream>>;
/// A future that resolves when the copy is complete.
pub type CopyCompleteFuture = WrappedFuture<Result<(), TransferError>>;
/// A future that resolves when the move is complete.
pub type MoveCompleteFuture = WrappedFuture<Result<(), TransferError>>;

pub(crate) struct BlockingStreamReader<S>
where
    S: Unpin + Stream,
{
    stream: BlockingStream<S>,
    reader: Option<Box<dyn io::Read>>,
}

impl<S> BlockingStreamReader<S>
where
    S: Unpin + Stream,
{
    pub fn from_stream(stream: S) -> BlockingStreamReader<S> {
        BlockingStreamReader {
            stream: block_on_stream(stream),
            reader: None,
        }
    }
}

impl<S, O, E> io::Read for BlockingStreamReader<S>
where
    S: Unpin + Stream<Item = Result<O, E>>,
    O: IntoBuf,
    <O as IntoBuf>::Buf: 'static,
    E: Into<StorageError>,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.reader {
            Some(ref mut r) => r.read(buf),
            None => match self.stream.next() {
                Some(Ok(d)) => {
                    let mut reader = d.into_buf().reader();
                    let count = reader.read(buf)?;
                    self.reader = Some(Box::new(reader));
                    Ok(count)
                }
                Some(Err(e)) => Err(e.into().into()),
                None => Ok(0),
            },
        }
    }
}
