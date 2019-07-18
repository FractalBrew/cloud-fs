use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::ready;
use futures::stream::{BoxStream, Stream};

use crate::types::*;
use crate::Fs;

pub(crate) type FsPoll<R> = Poll<FsResult<R>>;
pub(crate) type FsStreamPoll<R> = Poll<Option<FsResult<R>>>;

pub(crate) type FsPinned<R> = Pin<Box<dyn Future<Output = FsResult<R>> + Send + 'static>>;
pub(crate) type FsStreamPinned<R> = Pin<Box<dyn Stream<Item = FsResult<R>> + Send + 'static>>;

/// A Future whose error is always an [`FsError'](struct.FsError.html).
///
/// This is mostly used to hide the underlying futures in use which may change
/// frequently.
pub struct FsFuture<R>
where
    R: Send + 'static,
{
    base: FsPinned<R>,
}

impl<R> FsFuture<R>
where
    R: Send + 'static,
{
    pub(crate) fn from_future<F>(base: F) -> Self
    where
        F: Future<Output = FsResult<R>> + Send + 'static,
    {
        FsFuture {
            base: Box::pin(base),
        }
    }

    pub(crate) fn from_error<E>(error: E) -> Self
    where
        E: Into<FsError>,
    {
        FsFuture::from_future(ready(Err(error.into())))
    }
}

impl<R> Future for FsFuture<R>
where
    R: Send + 'static,
{
    type Output = FsResult<R>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> FsPoll<R> {
        self.base.as_mut().poll(cx)
    }
}

/// A Stream whose error is always an [`FsError'](struct.FsError.html).
///
/// This is mostly used to hide the underlying streams in use which may change
/// frequently.
pub struct FsStream<R>
where
    R: Send + 'static,
{
    base: FsStreamPinned<R>,
}

impl<R> FsStream<R>
where
    R: Send + 'static,
{
    pub(crate) fn from_stream<S>(base: S) -> Self
    where
        S: Stream<Item = FsResult<R>> + Send + 'static,
    {
        FsStream {
            base: Box::pin(base),
        }
    }
}

impl<R> Stream for FsStream<R>
where
    R: Send + 'static,
{
    type Item = FsResult<R>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> FsStreamPoll<R> {
        self.base.as_mut().poll_next(cx)
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

/// Holds an instance of `Stream`. This can be useful when you want to simplify
/// the types you're working with.
pub struct StreamHolder<R> {
    stream: BoxStream<'static, R>,
}

impl<R> StreamHolder<R> {
    /// Creates a new `StreamHolder` to hold a stream.
    pub fn new<S>(stream: S) -> StreamHolder<R>
    where
        S: Stream<Item = R> + Send + 'static,
    {
        StreamHolder {
            stream: Box::pin(stream),
        }
    }
}

impl<R> Stream for StreamHolder<R> {
    type Item = R;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<R>> {
        self.stream.as_mut().poll_next(cx)
    }
}

/// Merges a set of streams into a single stream that returns results whenever
/// they arrive, not necessarily in the order the streams were added.
///
/// Implements Stream, polling it will poll all the owned streams returning an
/// item if found. Once an owned stream returns None that stream will be
/// dropped. Once this stream returns None adding more streams will cause it to
/// start returning values again.
#[derive(Default)]
pub struct MergedStreams<R> {
    streams: Vec<Pin<Box<StreamHolder<R>>>>,
}

impl<R> MergedStreams<R> {
    /// Creates a new `MergedStreams`.
    pub fn new() -> MergedStreams<R> {
        MergedStreams {
            streams: Vec::new(),
        }
    }

    /// Adds a new stream to the set of streams polled.
    pub fn push<S>(&mut self, stream: S)
    where
        S: Stream<Item = R> + Send + 'static,
    {
        self.streams.push(Box::pin(StreamHolder::new(stream)));
    }
}

impl<R> Stream for MergedStreams<R> {
    type Item = R;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<R>> {
        if self.streams.is_empty() {
            return Poll::Ready(None);
        }

        let mut i = 0;
        while i < self.streams.len() {
            let stream = &mut self.streams[i];
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(r)) => return Poll::Ready(Some(r)),
                Poll::Ready(None) => {
                    self.streams.remove(i);
                    if self.streams.is_empty() {
                        return Poll::Ready(None);
                    }
                    continue;
                }
                Poll::Pending => {
                    // Move on to the next stream.
                }
            }
            i += 1;
        }

        Poll::Pending
    }
}
