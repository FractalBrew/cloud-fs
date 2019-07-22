//! A module with some useful tools for working with streams.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::Stream;

pub(crate) type StreamPoll<R> = Poll<Option<R>>;
pub(crate) type ResultStreamPoll<R> = StreamPoll<io::Result<R>>;

pub(crate) type PinnedStream<R> = Pin<Box<dyn Stream<Item = R> + Send + 'static>>;

/// Wraps a stream of an unknown type into a concrete type.
pub struct WrappedStream<R>
where
    R: Send + 'static,
{
    base: PinnedStream<R>,
}

impl<R> WrappedStream<R>
where
    R: Send + 'static,
{
    pub(crate) fn from_stream<S>(base: S) -> WrappedStream<S::Item>
    where
        S: Stream + Send + 'static,
        S::Item: Send,
    {
        WrappedStream {
            base: Box::pin(base),
        }
    }
}

impl<R> Stream for WrappedStream<R>
where
    R: Send + 'static,
{
    type Item = R;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> StreamPoll<R> {
        self.base.as_mut().poll_next(cx)
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
pub struct MergedStreams<R>
where
    R: Send + 'static,
{
    streams: Vec<Pin<Box<WrappedStream<R>>>>,
}

impl<R> MergedStreams<R>
where
    R: Send + 'static,
{
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
        self.streams
            .push(Box::pin(WrappedStream::<R>::from_stream(stream)));
    }
}

impl<R> Stream for MergedStreams<R>
where
    R: Send + 'static,
{
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
