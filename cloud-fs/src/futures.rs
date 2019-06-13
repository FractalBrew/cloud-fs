use tokio::prelude::*;

use crate::types::*;
use crate::Fs;

type FsPoll<R> = Poll<R, FsError>;
type FsStreamPoll<R> = Poll<Option<R>, FsError>;

/// A Future whose error is always an [`FsError'](struct.FsError.html).
///
/// This is mostly used to hide the underlying futures in use which may change
/// frequently.
pub struct FsFuture<R>
where
    R: Send + Sync + 'static,
{
    base: Box<Future<Item = R, Error = FsError> + Send + Sync>,
}

impl<R> FsFuture<R>
where
    R: Send + Sync + 'static,
{
    pub(crate) fn from_future<F>(base: F) -> Self
    where
        F: Future<Item = R, Error = FsError> + Sized + Send + Sync + 'static,
    {
        FsFuture {
            base: Box::new(base),
        }
    }

    pub(crate) fn from_item(item: R) -> Self {
        FsFuture::from_future(future::finished::<R, FsError>(item))
    }

    pub(crate) fn from_error(error: FsError) -> Self {
        FsFuture::from_future(future::err::<R, FsError>(error))
    }
}

impl<R> Future for FsFuture<R>
where
    R: Send + Sync + 'static,
{
    type Item = R;
    type Error = FsError;

    fn poll(&mut self) -> FsPoll<Self::Item> {
        self.base.poll()
    }
}

/// A Stream whose error is always an [`FsError'](struct.FsError.html).
///
/// This is mostly used to hide the underlying streams in use which may change
/// frequently.
pub struct FsStream<R>
where
    R: Send + Sync + 'static,
{
    base: Box<Stream<Item = R, Error = FsError> + Send + Sync>,
}

impl<R> FsStream<R>
where
    R: Send + Sync + 'static,
{
    pub(crate) fn from_stream<S>(base: S) -> Self
    where
        S: Stream<Item = R, Error = FsError> + Sized + Send + Sync + 'static,
    {
        FsStream {
            base: Box::new(base),
        }
    }
}

impl<R> Stream for FsStream<R>
where
    R: Send + Sync + 'static,
{
    type Item = R;
    type Error = FsError;

    fn poll(&mut self) -> FsStreamPoll<Self::Item> {
        self.base.poll()
    }
}

/// A stream that returns Bytes.
pub type DataStream = FsStream<Data>;
/// A future that returns a [`Fs`](struct.Fs.html) implementation.
pub type ConnectFuture = FsFuture<Fs>;
/// A stream that returns [`FsFile`s](struct.FsFile.html).
pub type FileListStream = FsStream<FsFile>;
/// A future that returns a [`FileListStream`](type.FileListStream.html).
pub type FileListFuture = FsFuture<FileListStream>;
/// A future that returns a [`FsFile`](type.FsFile.html).
pub type FileFuture = FsFuture<FsFile>;
/// A future that just resolves when whatever operation is complete.
pub type OperationCompleteFuture = FsFuture<()>;
/// A future that resolves to a [`DataStream`](type.DataStream.html).
pub type DataStreamFuture = FsFuture<DataStream>;
