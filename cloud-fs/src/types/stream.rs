use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::Stream;

use super::*;

pub(crate) type StreamPoll<R> = Poll<Option<R>>;
pub(crate) type FsStreamPoll<R> = StreamPoll<FsResult<R>>;

pub(crate) type StreamPinned<R> = Pin<Box<dyn Stream<Item = R> + Send + 'static>>;
pub(crate) type FsStreamPinned<R> = StreamPinned<FsResult<R>>;

/// A Stream whose error is always an [`FsError`](struct.FsError.html).
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

/// Holds an instance of `Stream`. This can be useful when you want to simplify
/// the types you're working with.
pub struct StreamHolder<R> {
    stream: StreamPinned<R>,
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

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> StreamPoll<R> {
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

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> StreamPoll<R> {
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
