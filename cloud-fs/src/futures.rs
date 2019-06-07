use tokio::prelude::*;

use crate::types::*;
use crate::Fs;

pub type FsPoll<R> = Poll<R, FsError>;
pub type FsStreamPoll<R> = Poll<Option<R>, FsError>;

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
    pub(crate) fn from_stream<S>(base: S) -> FsStream<R>
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

pub type DataStream = FsStream<Data>;
pub type ConnectFuture = FsFuture<Fs>;
pub type FileListStream = FsStream<FsFile>;
pub type FileFuture = FsFuture<FsFile>;
pub type OperationCompleteFuture = FsFuture<()>;
pub type DataStreamFuture = FsFuture<DataStream>;
